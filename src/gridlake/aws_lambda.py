"""AWS Lambda handlers.

``kinesis_handler`` is the production twin of ``StreamConsumer``: a Kinesis event
source mapping delivers ordered per-shard batches with at-least-once semantics and
checkpoints only when the handler returns. Raising makes Lambda retry (and bisect)
the batch, and ``ReportBatchItemFailures`` is not used on purpose: a batch is
committed to Iceberg atomically or not at all.
"""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from functools import cache
from typing import Any

import pyarrow as pa

from gridlake.catalog import Lakehouse, get_catalog
from gridlake.config import Settings
from gridlake.contracts import load_contracts
from gridlake.quality.engine import evaluate, quarantine_table

ALLOWED_LATENESS = timedelta(minutes=10)


@cache
def _lake() -> Lakehouse:
    settings = Settings()
    return Lakehouse(get_catalog(settings), load_contracts(settings.contracts_dir))


def kinesis_records_to_table(
    records: list[dict[str, Any]], now: datetime | None = None
) -> pa.Table:
    now = now or datetime.now(UTC)
    decoded = []
    for r in records:
        payload = json.loads(base64.b64decode(r["kinesis"]["data"]))
        decoded.append((r, payload, datetime.fromisoformat(payload["event_ts"])))
    # Per-invocation watermark: records are ordered per shard; Lambda invokes per shard.
    watermark = max((ts for *_, ts in decoded), default=now) - ALLOWED_LATENESS
    cols: dict[str, list] = {
        k: []
        for k in (
            "event_id",
            "substation_id",
            "ba_code",
            "event_ts",
            "frequency_hz",
            "load_mw",
            "voltage_kv",
            "_partition",
            "_offset",
            "_is_late",
            "_ingest_ts",
        )
    }
    for r, p, ts in decoded:
        for k in ("event_id", "substation_id", "ba_code", "frequency_hz", "load_mw", "voltage_kv"):
            cols[k].append(p.get(k))
        cols["event_ts"].append(ts)
        cols["_partition"].append(r["eventID"].split(":")[0])
        cols["_offset"].append(r["kinesis"]["sequenceNumber"])
        cols["_is_late"].append(ts < watermark)
        cols["_ingest_ts"].append(now)
    return pa.table(cols)


def kinesis_handler(event: dict, context: Any) -> dict:
    lake = _lake()
    table = kinesis_records_to_table(event["Records"])
    report = evaluate(lake.contract("bronze.grid_telemetry"), table)
    lake.append("bronze.grid_telemetry", report.valid)
    if report.quarantined.num_rows:
        lake.append(
            "quality.quarantine",
            quarantine_table(report, getattr(context, "aws_request_id", "lambda")),
        )
    return {"written": report.valid.num_rows, "quarantined": report.quarantined.num_rows}


def step_handler(event: dict, context: Any) -> dict:
    """Step Functions task: {"step": "silver", "since": "<execution start time>"}."""
    from gridlake.cli import main

    argv = ["step", event["step"]]
    for key in ("start", "end", "since"):
        if event.get(key):
            argv += [f"--{key}", event[key]]
    if event.get("no_telemetry", True):
        argv.append("--no-telemetry")
    code = main(argv)
    if code:
        raise RuntimeError(f"step {event['step']} failed")
    return {"step": event["step"], "status": "COMPLETE"}
