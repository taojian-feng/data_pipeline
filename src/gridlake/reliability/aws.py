"""Lambda entry point: EventBridge sends every Step Functions execution status change.

A failed execution, or a successful one whose quality results crossed the spike threshold,
is triaged. Drafts go to S3 (``GRIDLAKE_INCIDENTS_S3_URI``) and a summary goes to the alerts
topic. Nothing else changes: a person approves drafts with ``gridlake incidents approve``.
"""

from __future__ import annotations

import json
import os
from typing import Any

from gridlake.config import Settings
from gridlake.reliability import StepFunctionsRunLog, triage_run


def handler(event: dict[str, Any], context: Any, sns: Any = None) -> dict[str, Any]:
    detail = event.get("detail") or {}
    execution_arn = detail["executionArn"]
    settings = Settings()
    log = StepFunctionsRunLog(detail["stateMachineArn"], settings.aws_region)
    run, triages = triage_run(
        settings,
        run_id=execution_arn,
        clearance=os.getenv("GRIDLAKE_RELIABILITY_CLEARANCE", "internal"),
        min_rate=float(os.getenv("GRIDLAKE_RELIABILITY_MIN_RATE", "0.02")),
        log=log,
    )
    out = {
        "execution": execution_arn,
        "status": run.status,
        "incidents": [
            {
                "incident": t.incident.id,
                "headline": t.incident.headline(),
                "diagnosis": (t.diagnosis or {}).get("category"),
                "drafts": [d.id for d in t.drafts],
            }
            for t in triages
        ],
    }
    if triages and settings.alerts_topic_arn:
        if sns is None:
            import boto3  # optional dependency, only needed on AWS

            sns = boto3.client("sns", region_name=settings.aws_region)
        lines = [f"gridlake reliability agent: {len(triages)} incident(s) in {execution_arn}"]
        for t in triages:
            lines += ["", f"{t.incident.id}: {t.incident.headline()}", t.summary]
            lines += [f"  review: gridlake incidents show {d.id}" for d in t.drafts]
        sns.publish(
            TopicArn=settings.alerts_topic_arn,
            Subject=f"gridlake: {len(triages)} incident(s) triaged"[:100],
            Message="\n".join(lines),
        )
    print(json.dumps(out))
    return out
