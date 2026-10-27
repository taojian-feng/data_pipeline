"""Tools for the data reliability agent, built on the grid analyst's access-aware toolbox.

The agent investigates with read-only tools and can only *propose* changes:

* ``get_run_log``          the failing run: step statuses, errors, recent run history.
* ``get_quality_results``  rule outcomes for a table in this run, against its own history.
* ``inspect_quarantine``   what the rejected rows look like next to the accepted ones.
* ``get_lineage``          upstream jobs and datasets (and how they fared this run) and the
                           downstream blast radius.
* ``get_contract``         the data contract YAML: rules, owner, consumers, seed file.
* ``read_repo_file``       a contract or seed file, to draft an exact edit.
* ``run_sql`` / ``describe_table`` / ``list_tables`` from the grid analyst toolbox.
* ``record_diagnosis``     the category, cause and evidence (required before drafting).
* ``draft_issue`` / ``draft_fix``  pending drafts for a person to approve. A fix may only
                           touch ``contracts/*.yaml`` and ``seeds/*.csv``, must still parse,
                           must bump a changed contract's version, may never loosen a gold
                           rule, and a seed fix must pass its dimension's own quality rules.

Clearance is bound outside the model, as for the grid analyst. Rejected rows are as
sensitive as the table they were meant for, so ``inspect_quarantine`` checks the caller
against that table's classification.
"""

from __future__ import annotations

import csv
import difflib
import io
import json
import re
import shutil
import statistics
import tempfile
from collections import Counter
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import yaml

from gridlake.agent.tools import SPECS as ANALYST_SPECS
from gridlake.agent.tools import AccessDenied, SqlEngine, Toolbox, ToolResult
from gridlake.contracts import Contract, ContractError, _parse, cleared, metadata_classification
from gridlake.quality.engine import QualityGateError, evaluate
from gridlake.reliability.drafts import Draft, DraftStore
from gridlake.reliability.runlog import Incident, RunRecord, quality_for_run
from gridlake.transform import silver

CATEGORIES = (
    "source_data_defect",  # the feed sent wrong values (unit change, sign error, nulls)
    "reference_data_gap",  # a valid new code the reference dimension does not know yet
    "contract_out_of_date",  # the data is right and a rule's bound is too tight
    "schema_drift",  # columns added, removed or retyped upstream
    "transformation_bug",  # our own SQL produced bad rows from good inputs
    "upstream_outage",  # the source was unavailable or returned nothing
    "infrastructure",  # permissions, throttling, timeouts in our platform
    "unknown",
)
_EDITABLE = re.compile(r"^(contracts/[a-z0-9_.]+\.yaml|seeds/[a-z0-9_]+\.csv)$")
SEED_DIMENSIONS: dict[str, tuple[str, Callable[[Path], pa.Table]]] = {
    "seeds/fuel_types.csv": ("silver.dim_fuel_type", silver.dim_fuel_type),
    "seeds/balancing_authorities.csv": (
        "silver.dim_balancing_authority",
        silver.dim_balancing_authority,
    ),
}
MAX_DRAFTS = 3
_KEEP = {"list_tables", "run_sql", "describe_table"}

SPECS: list[dict[str, Any]] = [s for s in ANALYST_SPECS if s["name"] in _KEEP] + [
    {
        "name": "get_run_log",
        "description": (
            "The pipeline run behind the incident (or another run by id): each step's status, "
            "duration, metrics and error, plus whether recent runs passed. Start here."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"run_id": {"type": "string", "description": "default: this run"}},
            "additionalProperties": False,
        },
    },
    {
        "name": "get_quality_results",
        "description": (
            "Every quality rule evaluated on one table in this run (failed and total rows), "
            "with each rule's failure rate over earlier runs, so you can tell a spike from "
            "the usual background defect rate."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"table": {"type": "string", "description": "e.g. silver.demand_hourly"}},
            "required": ["table"],
            "additionalProperties": False,
        },
    },
    {
        "name": "inspect_quarantine",
        "description": (
            "Rows of one table that quality rules rejected in this run: counts per failed rule, "
            "a profile of the rule's column for rejected versus accepted rows (min, median, "
            "max, or the most common values), a breakdown by key columns such as ba_code, and "
            "a few sample records."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table": {"type": "string"},
                "rule": {"type": "string", "description": "only rows that failed this rule"},
                "limit": {"type": "integer", "description": "sample records, 0-10"},
            },
            "required": ["table"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_lineage",
        "description": (
            "Upstream datasets and jobs that feed a table, with the error rules that rejected "
            "rows of each in this run (and at what rate; under 1% is the usual background), and "
            "the downstream datasets and consumers it would affect."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"table": {"type": "string"}},
            "required": ["table"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_contract",
        "description": (
            "The data contract for a table as YAML (repo path, version, owner, rules, "
            "consumers), and the seed file a reference dimension is loaded from."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"table": {"type": "string"}},
            "required": ["table"],
            "additionalProperties": False,
        },
    },
    {
        "name": "read_repo_file",
        "description": "Read contracts/<table>.yaml or seeds/<name>.csv verbatim, to draft an exact edit.",
        "input_schema": {
            "type": "object",
            "properties": {"path": {"type": "string", "description": "e.g. seeds/fuel_types.csv"}},
            "required": ["path"],
            "additionalProperties": False,
        },
    },
    {
        "name": "record_diagnosis",
        "description": (
            "Record your conclusion before drafting anything: the category, the likely cause "
            "in one or two sentences, the evidence (each item a fact a tool returned), your "
            "confidence, and the team that should act."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "enum": list(CATEGORIES)},
                "cause": {"type": "string"},
                "evidence": {"type": "array", "items": {"type": "string"}},
                "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
                "owner": {"type": "string", "description": "team or address that should act"},
            },
            "required": ["category", "cause", "evidence", "confidence"],
            "additionalProperties": False,
        },
    },
    {
        "name": "draft_issue",
        "description": (
            "Draft a GitHub issue for a person to approve. Use it when the fix is outside this "
            "repo's contracts and seeds (a source defect, a transformation bug, an outage) or "
            "needs a human decision. Write the body in Markdown: what happened, evidence, "
            "impact, suggested next steps."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "body": {"type": "string"},
                "labels": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["title", "body"],
            "additionalProperties": False,
        },
    },
    {
        "name": "draft_fix",
        "description": (
            "Draft a change to contracts/*.yaml or seeds/*.csv for a person to approve, as exact "
            "text replacements (each 'old' must occur once in the file). A changed contract "
            "must bump its version; gold quality rules are never loosened. A seed change must "
            "pass its dimension's quality rules. Returns the unified diff."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "edits": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"old": {"type": "string"}, "new": {"type": "string"}},
                        "required": ["old", "new"],
                        "additionalProperties": False,
                    },
                },
                "title": {"type": "string"},
                "rationale": {"type": "string"},
            },
            "required": ["path", "edits", "title", "rationale"],
            "additionalProperties": False,
        },
    },
]


def rule_column(rule: str) -> str | None:
    """The column a rule name such as ``range(demand_mwh:0..250000)`` is about."""
    m = re.match(r"\w+\(([^:),\->]+)", rule or "")
    return m.group(1) if m else None


def _profile(values: list[Any]) -> dict[str, Any]:
    present = [v for v in values if v is not None]
    numbers = [float(v) for v in present if isinstance(v, int | float) and not isinstance(v, bool)]
    out: dict[str, Any] = {"rows": len(values), "nulls": len(values) - len(present)}
    if numbers and len(numbers) == len(present):
        out.update(
            min=round(min(numbers), 3),
            median=round(statistics.median(numbers), 3),
            max=round(max(numbers), 3),
        )
    elif present:
        out["top_values"] = [[str(v), n] for v, n in Counter(map(str, present)).most_common(8)]
    return out


def _semver(v: str) -> tuple[int, ...]:
    return tuple(int(p) for p in re.findall(r"\d+", str(v))[:3])


class ReliabilityToolbox(Toolbox):
    def __init__(
        self,
        contracts: dict[str, Contract],
        clearance: str,
        sql: SqlEngine,
        read: Callable[[str], pa.Table],
        incident: Incident,
        run: RunRecord,
        run_log: Any,
        drafts: DraftStore,
        contracts_dir: Path,
        seeds_dir: Path,
        edges: Callable[[], list[tuple[str, str, str]]],
        max_rows: int = 50,
    ):
        super().__init__(contracts, clearance, sql, max_rows=max_rows)
        self.read, self.incident, self.run, self.run_log = read, incident, run, run_log
        self.drafts, self.contracts_dir, self.seeds_dir = drafts, contracts_dir, seeds_dir
        self._edges, self._results = edges, None
        self.diagnosis: dict[str, Any] | None = None
        self.created: list[Draft] = []

    def specs(self) -> list[dict[str, Any]]:
        return SPECS

    # -- helpers ----------------------------------------------------------------
    def _quality(self) -> pa.Table:
        self._check_rows(self.contracts["quality.results"])
        if self._results is None:
            self._results = self.read("quality.results")
        return self._results

    def _failed_rules(self, table: str) -> list[dict[str, Any]]:
        """Error rules that rejected rows of ``table`` in this run, with their failure rate."""
        acc: dict[str, list[int]] = {}
        for r in quality_for_run(self._quality(), self.run).to_pylist():
            if r["table_name"] == table and r["severity"] == "error":
                a = acc.setdefault(r["rule"], [0, 0])
                a[0] += r["failed_rows"]
                a[1] += r["total_rows"]
        return [
            {"rule": rule, "failed_rows": f, "failure_rate": round(f / n, 4) if n else 0.0}
            for rule, (f, n) in sorted(acc.items())
            if f
        ]

    def _path(self, path: str) -> Path:
        path = path.strip()
        if not _EDITABLE.match(path):
            raise AccessDenied(f"{path!r}: only contracts/*.yaml and seeds/*.csv are in scope")
        base = self.contracts_dir if path.startswith("contracts/") else self.seeds_dir
        full = base / path.split("/", 1)[1]
        if not full.exists():
            raise ValueError(f"{path} does not exist")
        if path.startswith("contracts/"):
            self._check_metadata(self._contract(full.stem))
        return full

    def _footer(self) -> str:
        d = self.diagnosis or {}
        return (
            "\n\n---\n"
            f"Incident `{self.incident.id}` ({self.incident.kind}): {self.incident.headline()}\n"
            f"Run: `{self.run.run_id}` started {self.run.started}\n"
            f"Diagnosis: {d.get('category')} ({d.get('confidence')} confidence)\n"
            "_Drafted by the gridlake data reliability agent; filed only after a person approves._"
        )

    def _new_draft(self, **kwargs: Any) -> Draft:
        if self.diagnosis is None:
            raise ValueError("call record_diagnosis before drafting")
        if len(self.created) >= MAX_DRAFTS:
            raise ValueError(f"at most {MAX_DRAFTS} drafts per incident")
        draft = Draft(self.drafts.next_id(self.incident.id), self.incident.id, **kwargs)
        self.drafts.save(draft)
        self.created.append(draft)
        return draft

    # -- investigation tools ----------------------------------------------------
    def _tool_get_run_log(self, run_id: str | None = None) -> ToolResult:
        run = self.run if not run_id else self.run_log.get(run_id)
        out: dict[str, Any] = run.to_dict()
        out.pop("quality_run_ids", None)
        if hasattr(self.run_log, "runs"):
            out["recent_runs"] = [
                {"run_id": r.run_id, "started": r.started, "status": r.status}
                for r in self.run_log.runs()[-6:]
            ]
        return ToolResult(out)

    def _tool_get_quality_results(self, table: str) -> ToolResult:
        c = self._contract(table)
        self._check_metadata(c)
        all_rows = [r for r in self._quality().to_pylist() if r["table_name"] == c.name]
        in_scope = quality_for_run(self._quality(), self.run)
        this_run = {
            r["run_id"]
            for r in in_scope.select(["run_id", "table_name"]).to_pylist()
            if r["table_name"] == c.name
        }
        current: dict[str, dict] = {}
        history: dict[str, list[int]] = {}
        for r in all_rows:
            if r["run_id"] in this_run:
                cur = current.setdefault(
                    r["rule"],
                    {
                        "rule": r["rule"],
                        "severity": r["severity"],
                        "failed_rows": 0,
                        "total_rows": 0,
                    },
                )
                cur["failed_rows"] += r["failed_rows"]
                cur["total_rows"] += r["total_rows"]
            else:
                h = history.setdefault(r["rule"], [0, 0, 0, 0])
                h[0] += 1
                h[1] += int(r["failed_rows"] > 0)
                h[2] += r["failed_rows"]
                h[3] += r["total_rows"]
        rules = []
        for name, cur in sorted(current.items()):
            cur["failure_rate"] = (
                round(cur["failed_rows"] / cur["total_rows"], 4) if cur["total_rows"] else 0.0
            )
            h = history.get(name)
            cur["history"] = (
                {
                    "earlier_evaluations": h[0],
                    "evaluations_with_failures": h[1],
                    "failure_rate": round(h[2] / h[3], 4) if h[3] else 0.0,
                }
                if h
                else "first evaluation of this rule"
            )
            rules.append(cur)
        return ToolResult({"table": c.name, "rules": rules}, tables=["quality.results"])

    def _tool_inspect_quarantine(
        self, table: str, rule: str | None = None, limit: int = 5
    ) -> ToolResult:
        c = self._contract(table)
        self._check_rows(c)
        q = self.read("quality.quarantine")
        q = q.filter(pc.equal(q.column("table_name"), c.name))
        if self.run.quality_run_ids is not None:
            q = q.filter(pc.is_in(q.column("run_id"), pa.array(self.run.quality_run_ids)))
        else:
            ts = pa.scalar(datetime.fromisoformat(self.run.started), pa.timestamp("us", tz="UTC"))
            q = q.filter(pc.greater_equal(q.column("quarantined_ts"), ts))
        rows = q.to_pylist()
        if rule:
            rows = [r for r in rows if rule in r["failed_rules"]]
        records = [json.loads(r["record_json"]) for r in rows]
        out: dict[str, Any] = {
            "table": c.name,
            "quarantined_rows": len(rows),
            "by_failed_rules": Counter(r["failed_rules"] for r in rows).most_common(10),
        }
        focus = (rule or (rows[0]["failed_rules"] if rows else "")).split(";")[0]
        column = None if "," in focus.split(")")[0] else rule_column(focus)
        if column and column in c.column_names:
            accepted = self.read(c.name).column(column).to_pylist() if records else []
            out["column"] = column
            out["rejected_profile"] = _profile([r.get(column) for r in records])
            out["accepted_profile"] = _profile(accepted)
            key = next(
                (
                    k
                    for k in c.primary_key
                    if k != column and c.columns[c.column_names.index(k)].type == "string"
                ),
                None,
            )
            if key and "median" in out["rejected_profile"]:
                # The main entity's own accepted rows: did its values jump, or is it new?
                top, n = Counter(str(r.get(key)) for r in records).most_common(1)[0]
                t = self.read(c.name)
                t = t.filter(pc.equal(t.column(key), top))
                out["same_entity"] = {
                    "key": key,
                    "value": top,
                    "share_of_rejected_rows": round(n / len(records), 3),
                    "rejected": _profile(
                        [r.get(column) for r in records if str(r.get(key)) == top]
                    ),
                    "accepted": _profile(t.column(column).to_pylist()),
                }
                ts = [
                    k
                    for k in c.primary_key
                    if c.columns[c.column_names.index(k)].type in ("timestamp", "date")
                ]
                if ts:
                    mine = sorted(str(r[ts[0]]) for r in records if str(r.get(key)) == top)
                    out["same_entity"][f"rejected_{ts[0]}_range"] = [mine[0], mine[-1]]
            if "top_values" in out["rejected_profile"]:
                known = {str(v) for v in accepted}
                out["rejected_values_seen_in_accepted_rows"] = sorted(
                    v for v, _ in out["rejected_profile"]["top_values"] if v in known
                )
        keys = [
            k
            for k in c.primary_key
            if k != column and c.columns[c.column_names.index(k)].type == "string"
        ]
        out["by_key"] = {k: Counter(str(r.get(k)) for r in records).most_common(8) for k in keys}
        for k in c.primary_key:
            if c.columns[c.column_names.index(k)].type in ("timestamp", "date") and records:
                seen = sorted(str(r[k]) for r in records if r.get(k) is not None)
                if seen:
                    out[f"rejected_{k}_range"] = [seen[0], seen[-1]]
        out["samples"] = records[: max(0, min(int(limit), 10))]
        return ToolResult(out, tables=[c.name, "quality.quarantine"])

    def _tool_get_lineage(self, table: str) -> ToolResult:
        c = self._contract(table)
        self._check_metadata(c)
        edges = self._edges()

        def walk(start: str, up: bool) -> list[dict[str, Any]]:
            seen: dict[str, dict[str, Any]] = {}
            frontier, depth = [start], 0
            while frontier:
                depth += 1
                nxt = []
                for node in frontier:
                    for src, job, dst in edges:
                        a, b = (dst, src) if up else (src, dst)
                        if a == node and b not in seen and b != start:
                            seen[b] = {"dataset": b, "via_job": job, "depth": depth}
                            nxt.append(b)
                frontier = nxt
            return list(seen.values())

        def visible(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
            out = []
            for e in entries:
                dc = self.contracts.get(e["dataset"])
                if dc is None or not cleared(
                    metadata_classification(dc.classification), self.clearance
                ):
                    continue
                e["owner"] = dc.owner
                out.append(e)
            return out

        upstream = visible(walk(c.name, up=True))
        for e in upstream:
            e["failed_error_rules_this_run"] = self._failed_rules(e["dataset"])
        downstream = visible(walk(c.name, up=False))
        for e in downstream:
            e["consumers"] = list(self.contracts[e["dataset"]].consumers)
        return ToolResult(
            {
                "table": c.name,
                "owner": c.owner,
                "failed_error_rules_this_run": self._failed_rules(c.name),
                "upstream": upstream,
                "downstream": downstream,
                "consumers": list(c.consumers),
            }
        )

    def _tool_get_contract(self, table: str) -> ToolResult:
        c = self._contract(table)
        self._check_metadata(c)
        path = self.contracts_dir / f"{c.name}.yaml"
        out = {"path": f"contracts/{c.name}.yaml", "version": c.version, "yaml": path.read_text()}
        if c.source and c.source.startswith("seeds/"):
            out["seed_file"] = c.source.split()[0]
        return ToolResult(out, tables=[c.name])

    def _tool_read_repo_file(self, path: str) -> ToolResult:
        return ToolResult({"path": path.strip(), "text": self._path(path).read_text()})

    # -- conclusion tools -------------------------------------------------------
    def _tool_record_diagnosis(
        self,
        category: str,
        cause: str,
        evidence: list[str],
        confidence: str,
        owner: str | None = None,
    ) -> ToolResult:
        if category not in CATEGORIES:
            raise ValueError(f"category must be one of {CATEGORIES}")
        if confidence not in ("low", "medium", "high"):
            raise ValueError("confidence must be low, medium or high")
        if not evidence:
            raise ValueError("give at least one piece of evidence")
        self.diagnosis = {
            "category": category,
            "cause": cause.strip(),
            "evidence": [str(e) for e in evidence],
            "confidence": confidence,
            "owner": owner,
        }
        return ToolResult({"recorded": True})

    def _tool_draft_issue(
        self, title: str, body: str, labels: list[str] | None = None
    ) -> ToolResult:
        labels = sorted({"data-reliability", *(labels or [])})
        draft = self._new_draft(
            kind="issue", title=title.strip(), body=body.strip() + self._footer(), labels=labels
        )
        return ToolResult({"draft_id": draft.id, "status": draft.status})

    def _tool_draft_fix(
        self, path: str, edits: list[dict[str, str]], title: str, rationale: str
    ) -> ToolResult:
        path = path.strip()
        full = self._path(path)
        before = full.read_text()
        after = before
        if not edits:
            raise ValueError("give at least one edit")
        for e in edits:
            n = after.count(e["old"])
            if n != 1:
                raise ValueError(
                    f"'old' must occur exactly once in {path}; found {n}: {e['old'][:60]!r}"
                )
            after = after.replace(e["old"], e["new"])
        if after == before:
            raise ValueError("the edits change nothing")
        if path.startswith("contracts/"):
            self._validate_contract(full, before, after)
        else:
            self._validate_seed(path, after)
        patch = "".join(
            difflib.unified_diff(
                before.splitlines(keepends=True),
                after.splitlines(keepends=True),
                fromfile=f"a/{path}",
                tofile=f"b/{path}",
            )
        )
        draft = self._new_draft(
            kind="fix",
            title=title.strip(),
            body=rationale.strip() + self._footer(),
            labels=["data-reliability", "fix"],
            path=path,
            patch=patch,
        )
        return ToolResult({"draft_id": draft.id, "status": draft.status, "patch": patch})

    def _validate_contract(self, full: Path, before: str, after: str) -> None:
        old = _parse(yaml.safe_load(before), full)
        try:
            new = _parse(yaml.safe_load(after), full)
        except (ContractError, yaml.YAMLError) as e:
            raise ValueError(f"the edited contract does not parse: {e}") from e
        if new.name != old.name:
            raise ValueError("a fix cannot rename a contract")
        if _semver(new.version) <= _semver(old.version):
            raise ValueError(f"bump the contract version (currently {old.version})")
        if old.layer == "gold" and new.quality != old.quality:
            raise AccessDenied(
                "gold quality rules are not changed by the agent; a gold gate failure is a "
                "transformation bug, so draft an issue instead"
            )

    def _validate_seed(self, path: str, after: str) -> None:
        reader = list(csv.reader(io.StringIO(after)))
        header = next(csv.reader(io.StringIO((self.seeds_dir / Path(path).name).read_text())))
        if not reader or reader[0] != header:
            raise ValueError(f"the header of {path} must stay {header}")
        bad = [i for i, row in enumerate(reader[1:], 2) if row and len(row) != len(header)]
        if bad:
            raise ValueError(f"rows {bad} do not have {len(header)} fields")
        if path not in SEED_DIMENSIONS:
            return
        dim, build = SEED_DIMENSIONS[path]
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            for f in self.seeds_dir.glob("*.csv"):
                shutil.copy(f, tmp_dir / f.name)
            (tmp_dir / Path(path).name).write_text(after)
            try:
                table = self.contracts[dim].conform(build(tmp_dir))
                evaluate(self.contracts[dim], table, mode="strict")
            except (QualityGateError, ContractError, ValueError, KeyError) as e:
                raise ValueError(f"{dim} would reject the edited seed: {e}") from e
