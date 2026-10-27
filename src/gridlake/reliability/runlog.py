"""Run logs and incident detection for the data reliability agent.

An incident is either a failed pipeline step (the run alarmed) or an error-severity quality
rule that rejected an unusual share of a table's rows in one run (rows were quarantined, so
the run itself succeeded, but someone should look).

Locally the run log is the run manifest (``.gridlake/runs``) plus that run's OpenLineage
events. On AWS it is the Step Functions execution history; every task there runs its own
pipeline instance, so quality results are scoped by the execution's time window instead of
by run id.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

_GATE = re.compile(r"quality gate failed for ([a-z_]+\.[a-z0-9_]+): (.+)")
_RULE = re.compile(r"(\S+\(.*?\)|\S+) \((\d+) rows\)")


@dataclass
class RunStep:
    name: str
    status: str
    seconds: float | None = None
    metrics: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass
class RunRecord:
    run_id: str
    started: str
    status: str  # SUCCEEDED | FAILED
    steps: list[RunStep]
    source: str  # local | stepfunctions
    ended: str | None = None
    # Quality results belong to this run if their run_id is listed, else if they were
    # evaluated between started and ended.
    quality_run_ids: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Incident:
    id: str
    kind: str  # step_failed | quality_spike
    run_id: str
    started: str
    step: str | None = None
    table: str | None = None
    rule: str | None = None
    failed_rows: int = 0
    total_rows: int = 0
    error: str | None = None

    @property
    def failure_rate(self) -> float:
        return self.failed_rows / self.total_rows if self.total_rows else 0.0

    def headline(self) -> str:
        if self.kind == "step_failed":
            return f"step {self.step} failed: {self.error}"
        return (
            f"{self.table}: {self.rule} rejected {self.failed_rows} of {self.total_rows} rows "
            f"({self.failure_rate:.1%})"
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Incident:
        return cls(**{k: d[k] for k in cls.__dataclass_fields__ if k in d})


# -- run logs -------------------------------------------------------------------


class LocalRunLog:
    """Run manifests and OpenLineage events written by ``gridlake run``."""

    def __init__(self, runs_dir: Path, lineage_dir: Path):
        self.runs_dir, self.lineage_dir = runs_dir, lineage_dir

    def runs(self) -> list[RunRecord]:
        return sorted(
            (self._record(p) for p in self.runs_dir.glob("*.json")), key=lambda r: r.started
        )

    def latest(self) -> RunRecord:
        runs = self.runs()
        if not runs:
            raise LookupError(f"no runs in {self.runs_dir}; run `gridlake run` first")
        return runs[-1]

    def get(self, run_id: str) -> RunRecord:
        matches = [p for p in self.runs_dir.glob(f"{run_id}*.json")]
        if len(matches) != 1:
            raise LookupError(f"run {run_id!r} not found (or ambiguous) in {self.runs_dir}")
        return self._record(matches[0])

    def events(self, run_id: str) -> list[dict[str, Any]]:
        path = self.lineage_dir / f"{run_id}.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def _record(self, path: Path) -> RunRecord:
        m = json.loads(path.read_text())
        # FAIL lineage events carry the exception type, which the manifest drops.
        fail_messages = {
            e["job"]["name"]: e["run"]["facets"]["errorMessage"]["message"]
            for e in self.events(m["run_id"])
            if e["eventType"] == "FAIL" and "errorMessage" in e["run"]["facets"]
        }
        steps = [
            RunStep(
                s["name"],
                s["status"],
                s.get("seconds"),
                {k: v for k, v in s.get("metrics", {}).items() if k != "error"},
                fail_messages.get(s["name"]) or s.get("metrics", {}).get("error"),
            )
            for s in m["steps"]
        ]
        return RunRecord(
            m["run_id"],
            m["started"],
            "SUCCEEDED" if m["ok"] else "FAILED",
            steps,
            "local",
            quality_run_ids=[m["run_id"]],
        )


class StepFunctionsRunLog:
    """The run log of one Step Functions execution: which states ran, and why one failed."""

    def __init__(self, state_machine_arn: str, region: str, client: Any = None):
        if client is None:
            import boto3  # optional dependency, only needed on AWS

            client = boto3.client("stepfunctions", region_name=region)
        self.client, self.state_machine_arn = client, state_machine_arn

    def latest(self) -> RunRecord:
        execs = self.client.list_executions(stateMachineArn=self.state_machine_arn, maxResults=1)
        if not execs["executions"]:
            raise LookupError("the state machine has no executions")
        return self.get(execs["executions"][0]["executionArn"])

    def get(self, execution_arn: str) -> RunRecord:
        ex = self.client.describe_execution(executionArn=execution_arn)
        history: list[dict] = []
        kwargs: dict[str, Any] = {"executionArn": execution_arn, "maxResults": 1000}
        while True:
            page = self.client.get_execution_history(**kwargs)
            history += page["events"]
            if not page.get("nextToken"):
                break
            kwargs["nextToken"] = page["nextToken"]
        steps: dict[str, RunStep] = {}
        entered: dict[int, str] = {}
        current = None
        for ev in history:
            t = ev["type"]
            if t.endswith("StateEntered"):
                current = ev["stateEnteredEventDetails"]["name"]
                entered[ev["id"]] = current
                steps[current] = RunStep(current, "RUNNING")
            elif t.endswith("StateExited"):
                name = ev["stateExitedEventDetails"]["name"]
                if steps.get(name) and steps[name].status == "RUNNING":
                    steps[name].status = "COMPLETE"
            elif t in ("TaskFailed", "LambdaFunctionFailed", "ExecutionFailed") and current:
                details = next((v for k, v in ev.items() if k.endswith("EventDetails")), {})
                steps[current].status = "FAIL"
                steps[current].error = _lambda_error(details)
        for s in steps.values():  # a state still RUNNING when the execution failed failed
            if s.status == "RUNNING" and ex["status"] != "RUNNING":
                s.status = "FAIL" if ex["status"] != "SUCCEEDED" else "COMPLETE"
        return RunRecord(
            ex["executionArn"],
            _iso(ex["startDate"]),
            ex["status"],
            [s for s in steps.values() if s.name not in ("NotifyFailure", "Failed", "Done")],
            "stepfunctions",
            ended=_iso(ex["stopDate"]) if ex.get("stopDate") else None,
        )

    def events(self, run_id: str) -> list[dict[str, Any]]:
        return []  # Lambda keeps lineage events in /tmp; the execution history is the log


def _iso(v: Any) -> str:
    return v.isoformat() if isinstance(v, datetime) else str(v)


def _lambda_error(details: dict[str, Any]) -> str:
    """``TaskFailed`` from ``lambda:invoke`` carries the Lambda error as JSON in ``cause``."""
    cause = details.get("cause") or ""
    try:
        payload = json.loads(cause)
        return f"{payload.get('errorType', details.get('error'))}: {payload.get('errorMessage')}"
    except (ValueError, TypeError, AttributeError):
        return f"{details.get('error', 'Error')}: {cause}".strip(": ")


# -- detection ------------------------------------------------------------------


def quality_for_run(results: pa.Table, run: RunRecord) -> pa.Table:
    """Rows of ``quality.results`` produced by ``run``."""
    if run.quality_run_ids is not None:
        return results.filter(pc.is_in(results.column("run_id"), pa.array(run.quality_run_ids)))
    ts = results.column("evaluated_ts")
    start = pa.scalar(datetime.fromisoformat(run.started), pa.timestamp("us", tz="UTC"))
    mask = pc.greater_equal(ts, start)
    if run.ended:
        end = pa.scalar(datetime.fromisoformat(run.ended), pa.timestamp("us", tz="UTC"))
        mask = pc.and_(mask, pc.less_equal(ts, end))
    return results.filter(mask)


def detect_incidents(
    run: RunRecord, quality: pa.Table, min_rate: float = 0.02, min_rows: int = 5
) -> list[Incident]:
    """Failed steps, then error rules that rejected at least ``min_rate`` of a table's rows
    (and at least ``min_rows`` rows) in this run. Baseline defect rates stay under the bar."""
    incidents: list[Incident] = []
    gated: set[str] = set()
    for s in run.steps:
        if s.status != "FAIL":
            continue
        inc = Incident("", "step_failed", run.run_id, run.started, step=s.name, error=s.error)
        if m := _GATE.search(s.error or ""):
            inc.table = m.group(1)
            gated.add(inc.table)
            if r := _RULE.search(m.group(2)):
                inc.rule, inc.failed_rows = r.group(1), int(r.group(2))
        incidents.append(inc)
    sums: dict[tuple[str, str], list[int]] = {}
    for row in quality.to_pylist():
        if row["severity"] != "error" or row["table_name"] in gated:
            continue
        acc = sums.setdefault((row["table_name"], row["rule"]), [0, 0])
        acc[0] += row["failed_rows"]
        acc[1] += row["total_rows"]
    for (table, rule), (failed, total) in sorted(sums.items()):
        if failed >= min_rows and total and failed / total >= min_rate:
            incidents.append(
                Incident(
                    "",
                    "quality_spike",
                    run.run_id,
                    run.started,
                    table=table,
                    rule=rule,
                    failed_rows=failed,
                    total_rows=total,
                )
            )
    short = re.sub(r"[^A-Za-z0-9]", "", run.run_id.rsplit(":", 1)[-1])[:8]
    for n, inc in enumerate(incidents, 1):
        inc.id = f"inc-{short}-{n}"
    return incidents


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")
