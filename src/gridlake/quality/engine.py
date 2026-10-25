"""Declarative data-quality engine driven by data contracts.

Rules are compiled to DuckDB SQL and evaluated against Arrow tables in-process.

Row-level rules (``not_null``, ``range``, ``accepted_values``, ``referential``,
``unique``, ``expression``) identify the offending rows. Dataset-level rules
(``row_count_min``, ``freshness``) judge the batch as a whole.

Gate behaviour by severity:
  * ``warn``  - recorded in ``quality.results``; data flows.
  * ``error`` - in ``quarantine`` mode offending rows are diverted to
    ``quality.quarantine`` and the rest flows; a failing dataset-level rule, or any
    error in ``strict`` mode, raises ``QualityGateError`` and stops the pipeline
    before anything is published (circuit breaker).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from gridlake.contracts import Contract
from gridlake.transform.sql import connect

RID = "__rid"
ROW_RULES = {"not_null", "range", "accepted_values", "referential", "unique", "expression"}
DATASET_RULES = {"row_count_min", "freshness"}


class QualityGateError(RuntimeError):
    def __init__(self, report: QualityReport):
        failed = [r for r in report.results if not r.passed and r.severity == "error"]
        super().__init__(
            f"quality gate failed for {report.table}: "
            + "; ".join(f"{r.rule} ({r.failed_rows} rows)" for r in failed)
        )
        self.report = report


@dataclass
class RuleResult:
    rule: str
    severity: str
    failed_rows: int
    total_rows: int

    @property
    def passed(self) -> bool:
        return self.failed_rows == 0


@dataclass
class QualityReport:
    table: str
    results: list[RuleResult]
    valid: pa.Table
    quarantined: pa.Table
    quarantine_reasons: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed or r.severity == "warn" for r in self.results)

    def metrics(self) -> dict[str, Any]:
        return {
            "rowCount": self.valid.num_rows + self.quarantined.num_rows,
            "validRows": self.valid.num_rows,
            "quarantinedRows": self.quarantined.num_rows,
            "assertions": [
                {
                    "assertion": r.rule,
                    "success": r.passed,
                    "severity": r.severity,
                    "failedRows": r.failed_rows,
                }
                for r in self.results
            ],
        }


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _lit(v: Any) -> str:
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return repr(v)


def rule_name(rule: dict[str, Any]) -> str:
    kind = rule["rule"]
    if kind in ("not_null", "unique"):
        return f"{kind}({','.join(rule['columns'])})"
    if kind == "range":
        return f"range({rule['column']}:{rule.get('min')}..{rule.get('max')})"
    if kind in ("accepted_values", "referential"):
        target = rule.get("ref_table", "") and f"->{rule['ref_table']}.{rule['ref_column']}"
        return f"{kind}({rule['column']}{target})"
    if kind == "expression":
        return f"expression({rule['sql']})"
    if kind == "row_count_min":
        return f"row_count_min({rule['min']})"
    return kind


def _failing_sql(rule: dict[str, Any]) -> str:
    kind = rule["rule"]
    if kind == "not_null":
        cond = " OR ".join(f"{_q(c)} IS NULL" for c in rule["columns"])
        return f"SELECT {RID} FROM t WHERE {cond}"
    if kind == "range":
        col, parts = _q(rule["column"]), []
        if "min" in rule:
            parts.append(f"{col} < {_lit(rule['min'])}")
        if "max" in rule:
            parts.append(f"{col} > {_lit(rule['max'])}")
        return f"SELECT {RID} FROM t WHERE {col} IS NOT NULL AND ({' OR '.join(parts)})"
    if kind == "accepted_values":
        col = _q(rule["column"])
        values = ", ".join(_lit(v) for v in rule["values"])
        return f"SELECT {RID} FROM t WHERE {col} IS NOT NULL AND {col} NOT IN ({values})"
    if kind == "referential":
        col, ref = _q(rule["column"]), _q(rule["ref_column"])
        return f"SELECT {RID} FROM t WHERE {col} IS NOT NULL AND {col} NOT IN (SELECT {ref} FROM ref WHERE {ref} IS NOT NULL)"
    if kind == "unique":
        cols = ", ".join(_q(c) for c in rule["columns"])
        return f"SELECT {RID} FROM (SELECT {RID}, count(*) OVER (PARTITION BY {cols}) AS n FROM t) WHERE n > 1"
    if kind == "expression":
        return f"SELECT {RID} FROM t WHERE NOT coalesce(({rule['sql']}), false)"
    raise ValueError(f"unknown row rule {kind}")


def evaluate(
    contract: Contract,
    data: pa.Table,
    reference_loader: Callable[[str], pa.Table] | None = None,
    mode: str = "quarantine",
    now: datetime | None = None,
) -> QualityReport:
    """Evaluate every rule in ``contract`` against ``data``; raise if the gate fails."""
    n = data.num_rows
    con = connect(t=data.append_column(RID, pa.array(np.arange(n, dtype=np.int64))))
    # Required (non-nullable) contract columns imply a not_null rule, so bad rows are
    # quarantined instead of failing the schema check at write time.
    required = [c.name for c in contract.columns if not c.nullable and c.name in data.column_names]
    rules = (
        [{"rule": "not_null", "columns": required, "severity": "error"}] if required else []
    ) + list(contract.quality)
    if contract.freshness:
        rules.append({"rule": "freshness", **contract.freshness})

    results: list[RuleResult] = []
    bad = np.zeros(n, dtype=bool)
    reasons: list[list[str]] = [[] for _ in range(n)]
    dataset_error = False

    for rule in rules:
        kind, severity, name = rule["rule"], rule.get("severity", "error"), rule_name(rule)
        if kind in ROW_RULES:
            if kind == "referential":
                if reference_loader is None:
                    continue
                con.register("ref", reference_loader(rule["ref_table"]))
            rids = np.array(
                [r[0] for r in con.execute(_failing_sql(rule)).fetchall()], dtype=np.int64
            )
            results.append(RuleResult(name, severity, len(rids), n))
            if severity == "error" and len(rids):
                if mode == "strict":
                    dataset_error = True
                bad[rids] = True
                for i in rids:
                    reasons[i].append(name)
        elif kind == "row_count_min":
            failed = int(n < rule["min"])
            results.append(RuleResult(name, severity, failed, n))
            dataset_error |= bool(failed and severity == "error")
        elif kind == "freshness":
            now = now or datetime.now(UTC)
            latest = pc.max(data.column(rule["column"])).as_py() if n else None
            lag_h = (now - latest).total_seconds() / 3600 if latest else float("inf")
            failed = int(lag_h > rule["max_lag_hours"])
            results.append(
                RuleResult(
                    f"freshness({rule['column']}<={rule['max_lag_hours']}h)", severity, failed, n
                )
            )
            dataset_error |= bool(failed and severity == "error")
        else:
            raise ValueError(f"unknown rule {kind!r} in {contract.name}")

    keep = pa.array(~bad)
    report = QualityReport(
        table=contract.name,
        results=results,
        valid=data.filter(keep),
        quarantined=data.filter(pa.array(bad)),
        quarantine_reasons=["; ".join(r) for r, b in zip(reasons, bad, strict=True) if b],
    )
    if dataset_error:
        raise QualityGateError(report)
    return report


def results_table(report: QualityReport, run_id: str) -> pa.Table:
    now = datetime.now(UTC)
    return pa.table(
        {
            "run_id": [run_id] * len(report.results),
            "table_name": [report.table] * len(report.results),
            "rule": [r.rule for r in report.results],
            "severity": [r.severity for r in report.results],
            "failed_rows": [r.failed_rows for r in report.results],
            "total_rows": [r.total_rows for r in report.results],
            "passed": [r.passed for r in report.results],
            "evaluated_ts": pa.array([now] * len(report.results), pa.timestamp("us", tz="UTC")),
        }
    )


def quarantine_table(report: QualityReport, run_id: str) -> pa.Table:
    now = datetime.now(UTC)
    rows = report.quarantined.to_pylist()
    return pa.table(
        {
            "run_id": [run_id] * len(rows),
            "table_name": [report.table] * len(rows),
            "failed_rules": report.quarantine_reasons,
            "record_json": [json.dumps(r, default=str, sort_keys=True) for r in rows],
            "quarantined_ts": pa.array([now] * len(rows), pa.timestamp("us", tz="UTC")),
        }
    )
