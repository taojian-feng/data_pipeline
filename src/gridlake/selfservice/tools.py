"""Tools for the self-service data product agent, built on the grid analyst's toolbox.

The agent explores and previews with read-only tools, and can only *propose* a product:

* ``list_tables`` / ``describe_table`` / ``run_sql`` from the grid analyst toolbox.
* ``get_contract``          an existing contract's YAML, to follow the house conventions.
* ``preview_transform``     run a draft SELECT over the governed tables the caller may read:
                            output columns and types, null counts, sample rows, and the
                            classification the inputs imply.
* ``draft_data_product``    a pending draft of three new files (contract, SQL, tests). The tool,
                            not the model, decides what is derived: the inputs come from the
                            SQL, the owner from the request, the classification may not be
                            below any input's. It builds the product from today's tables, and
                            only a draft that meets the governance rules (ADR 0008), conforms
                            to its contract, passes its quality rules and its own checks is saved.

Clearance and owner are bound outside the model, as for the other agents.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import yaml

from gridlake.agent.tools import SPECS as ANALYST_SPECS
from gridlake.agent.tools import SqlEngine, Toolbox, ToolResult, referenced_tables
from gridlake.contracts import CLASSIFICATIONS, Contract, ContractError, _parse
from gridlake.quality.engine import QualityGateError, evaluate
from gridlake.reliability.drafts import Draft, DraftStore
from gridlake.selfservice import drafting
from gridlake.transform.declared import (
    GRAINS,
    SOURCE_LAYERS,
    check_product,
    period_start,
    query_tables,
    run_checks,
    strip_comments,
)

MAX_DRAFTS = 2
TESTS_DIR = "tests/products"
_NAME = re.compile(r"^gold\.[a-z][a-z0-9_]{2,60}$")
_TYPES = {
    "string": "string",
    "large_string": "string",
    "double": "double",
    "float": "double",
    "int64": "long",
    "int32": "int",
    "bool": "boolean",
    "date32[day]": "date",
}
_KEEP = {"list_tables", "run_sql", "describe_table"}

_COLUMN = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "type": {"type": "string", "enum": ["string", "double", "long", "int", "boolean", "date"]},
        "description": {"type": "string"},
        "nullable": {"type": "boolean", "description": "default true; key columns are required"},
        "business_term": {"type": "string", "description": "glossary term, if one applies"},
    },
    "required": ["name", "type", "description"],
    "additionalProperties": False,
}

SPECS: list[dict[str, Any]] = [s for s in ANALYST_SPECS if s["name"] in _KEEP] + [
    {
        "name": "get_contract",
        "description": (
            "The data contract YAML of an existing table: owner, classification, grain, "
            "columns with types and descriptions, quality rules. Read one or two before drafting."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"table": {"type": "string"}},
            "required": ["table"],
            "additionalProperties": False,
        },
    },
    {
        "name": "preview_transform",
        "description": (
            "Run a draft SELECT over silver and gold tables and see its output: columns with "
            "contract types, null counts, row count, sample rows, and the minimum "
            "classification its inputs imply. Iterate here until the output is right."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"sql": {"type": "string", "description": "One SELECT statement"}},
            "required": ["sql"],
            "additionalProperties": False,
        },
    },
    {
        "name": "draft_data_product",
        "description": (
            "Draft the new gold data product for a person to approve: the contract, the SQL "
            "and tests, as one patch. The SQL must return the period start date (start of the "
            "day, week or month, cast to DATE) and exactly the declared columns. The inputs are "
            "the tables the SQL reads; the owner is the requester. The draft is built from "
            "today's tables and is only saved if it meets the governance rules, conforms to its "
            "contract, and passes its quality rules and checks. Errors say what to fix."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "gold.<snake_case_name>"},
                "description": {
                    "type": "string",
                    "description": "what one row is, what it is for, who uses it",
                },
                "classification": {"type": "string", "enum": list(CLASSIFICATIONS)},
                "consumers": {"type": "array", "items": {"type": "string"}},
                "grain": {"type": "string", "enum": list(GRAINS)},
                "primary_key": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "the period date column first, then the entity keys",
                },
                "columns": {"type": "array", "items": _COLUMN},
                "sql": {"type": "string"},
                "quality": {
                    "type": "array",
                    "description": (
                        "extra rules as in contracts/: {rule: range, column, min, max}, "
                        "{rule: not_null, columns}, {rule: accepted_values, column, values}, "
                        "{rule: expression, sql, description}. unique(primary key) is added."
                    ),
                    "items": {"type": "object"},
                },
                "checks": {
                    "type": "array",
                    "description": (
                        "tests: each a SELECT over the product and its inputs that returns the "
                        "rows violating an expectation, e.g. totals that do not reconcile"
                    ),
                    "items": {
                        "type": "object",
                        "properties": {"name": {"type": "string"}, "sql": {"type": "string"}},
                        "required": ["name", "sql"],
                        "additionalProperties": False,
                    },
                },
                "domain": {"type": "string"},
                "rationale": {
                    "type": "string",
                    "description": "for the reviewer: choices made and anything to double-check",
                },
            },
            "required": [
                "name",
                "description",
                "classification",
                "consumers",
                "grain",
                "primary_key",
                "columns",
                "sql",
                "checks",
                "rationale",
            ],
            "additionalProperties": False,
        },
    },
]


def contract_type(t: pa.DataType) -> str:
    return _TYPES.get(str(t), str(t))


def _jsonable(v: Any) -> Any:
    if isinstance(v, float):
        return round(v, 3)
    if isinstance(v, date):
        return v.isoformat()
    return v


class ProductToolbox(Toolbox):
    def __init__(
        self,
        contracts: dict[str, Contract],
        clearance: str,
        sql: SqlEngine,
        read: Callable[[str], pa.Table],
        request: str,
        owner: str,
        drafts: DraftStore,
        repo_root: Path,
        contracts_dir: Path,
        transforms_dir: Path,
        max_rows: int = 50,
    ):
        super().__init__(contracts, clearance, sql, max_rows=max_rows)
        self.read, self.request, self.owner, self.drafts = read, request, owner, drafts
        self.repo_root, self.contracts_dir, self.transforms_dir = (
            repo_root,
            contracts_dir,
            transforms_dir,
        )
        self.created: list[Draft] = []

    def specs(self) -> list[dict[str, Any]]:
        return SPECS

    # -- helpers ----------------------------------------------------------------
    def _inputs(self, sql: str) -> list[Contract]:
        names = referenced_tables(strip_comments(sql))
        if not names:
            raise ValueError("the SQL reads no governed table")
        out = []
        for name in sorted(names):
            c = self._contract(name)
            if c.layer not in SOURCE_LAYERS:
                raise ValueError(
                    f"{c.name} is {c.layer}; data products read conformed silver or gold tables"
                )
            self._check_rows(c)
            out.append(c)
        return out

    def _run(self, sql: str, inputs: list[Contract]) -> pa.Table:
        return query_tables(sql, {c.name: self.read(c.name) for c in inputs})

    # -- tools ------------------------------------------------------------------
    def _tool_get_contract(self, table: str) -> ToolResult:
        c = self._contract(table)
        self._check_metadata(c)
        path = self.contracts_dir / f"{c.name}.yaml"
        return ToolResult(
            {"path": f"contracts/{c.name}.yaml", "yaml": path.read_text()}, tables=[c.name]
        )

    def _tool_preview_transform(self, sql: str) -> ToolResult:
        inputs = self._inputs(sql)
        data = self._run(sql, inputs)
        floor = max(inputs, key=lambda c: CLASSIFICATIONS.index(c.classification))
        return ToolResult(
            {
                "rows": data.num_rows,
                "columns": [
                    {
                        "name": f.name,
                        "type": contract_type(f.type),
                        "nulls": data.column(f.name).null_count,
                    }
                    for f in data.schema
                ],
                "sample": [
                    [_jsonable(v) for v in r.values()] for r in data.slice(0, 5).to_pylist()
                ],
                "inputs": [c.name for c in inputs],
                "minimum_classification": floor.classification,
                "because_of": floor.name,
            },
            tables=[c.name for c in inputs],
        )

    def _tool_draft_data_product(
        self,
        name: str,
        description: str,
        classification: str,
        consumers: list[str],
        grain: str,
        primary_key: list[str],
        columns: list[dict[str, Any]],
        sql: str,
        checks: list[dict[str, str]],
        rationale: str,
        quality: list[dict[str, Any]] | None = None,
        domain: str | None = None,
    ) -> ToolResult:
        if len(self.created) >= MAX_DRAFTS:
            raise ValueError(f"at most {MAX_DRAFTS} drafts per request")
        name = name.strip().lower()
        if not _NAME.match(name):
            raise ValueError("name must look like gold.<snake_case>, 3-60 characters")
        files = {
            f"contracts/{name}.yaml": self.contracts_dir / f"{name}.yaml",
            f"transforms/{name}.sql": self.transforms_dir / f"{name}.sql",
            f"{TESTS_DIR}/{drafting.test_module_name(name)}.py": self.repo_root
            / TESTS_DIR
            / f"{drafting.test_module_name(name)}.py",
        }
        if name in self.contracts or any(p.exists() for p in files.values()):
            raise ValueError(f"{name} already exists; choose another name")
        # Validate the file exactly as it will be committed, header comment included.
        sql = drafting.render_sql(name, self.request, sql)
        inputs = self._inputs(sql)
        if not checks:
            raise ValueError("give at least one check, e.g. that totals reconcile with the input")
        dates = [c["name"] for c in columns if c["name"] in primary_key and c["type"] == "date"]
        if len(dates) != 1:
            raise ValueError("the primary key needs exactly one date column: the period start")
        rules = [dict(r) for r in quality or []]
        for r in rules:
            r.setdefault("severity", "error")
        if not any(r.get("rule") == "unique" for r in rules):
            rules.insert(0, {"rule": "unique", "columns": list(primary_key), "severity": "error"})
        cols = []
        for col in columns:
            entry: dict[str, Any] = {"name": col["name"], "type": col["type"]}
            if col["name"] in primary_key or col.get("nullable") is False:
                entry["nullable"] = False
            entry["description"] = " ".join(col["description"].split())
            if col.get("business_term"):
                entry["business_term"] = col["business_term"]
            cols.append(entry)
        spec = {
            "name": name,
            "version": "1.0.0",
            "layer": "gold",
            "domain": domain or inputs[0].domain or "grid-operations",
            "owner": self.owner,
            "classification": classification,
            "description": description,
            "primary_key": list(primary_key),
            "partition_by": [{"column": dates[0], "transform": "identity"}],
            "consumers": list(consumers),
            "transform": {
                "sql": f"transforms/{name}.sql",
                "inputs": [c.name for c in inputs],
                "grain": grain,
            },
            "columns": cols,
            "quality": rules,
        }
        text = drafting.render_contract(spec)
        try:
            loaded = yaml.safe_load(text)
            contract = _parse(loaded, Path(f"{name}.yaml"))
        except (ContractError, yaml.YAMLError, TypeError) as e:
            raise ValueError(f"the contract does not parse: {e}") from e

        def norm(d: dict[str, Any]) -> dict[str, Any]:
            return {**d, "description": " ".join(str(d.get("description", "")).split())}

        if norm(loaded) != norm(spec):
            raise ValueError("the contract YAML does not round-trip; simplify descriptions")
        problems = check_product(contract, {**self.contracts, name: contract}, sql)
        if problems:
            raise ValueError("governance: " + "; ".join(problems))

        # Build it from today's tables, exactly as the pipeline will.
        data = self._run(sql, inputs)
        got, want = data.column_names, contract.column_names
        if sorted(got) != sorted(want):
            raise ValueError(
                f"the SQL returns {got} but the contract declares {want}; make them match"
            )
        if not data.num_rows:
            raise ValueError(
                "the SQL returns no rows from today's tables, so it cannot be verified"
            )
        period = dates[0]
        values = data.column(period).cast(pa.date32()).to_pylist()
        bad = sorted({v for v in values if v is not None and period_start(v, grain) != v})
        if bad:
            raise ValueError(f"{period} must be the start of each {grain}; got {bad[:3]}")
        try:
            table = contract.conform(data)
            evaluate(contract, table, reference_loader=self.read, mode="strict")
        except ContractError as e:
            raise ValueError(f"the output does not conform: {e}") from e
        except QualityGateError as e:
            raise ValueError(f"today's data fails the drafted rules: {e}") from e
        named = {c["name"].strip(): c["sql"] for c in checks}
        try:
            failures = run_checks(
                named, {c.name: self.read(c.name) for c in inputs} | {name: table}
            )
        except Exception as e:
            raise ValueError(f"a check does not run: {e}") from e
        failing = {k: v for k, v in failures.items() if v}
        if failing:
            raise ValueError(f"checks fail on today's data (violating rows): {failing}")

        texts = {
            f"contracts/{name}.yaml": text,
            f"transforms/{name}.sql": sql,
            f"{TESTS_DIR}/{drafting.test_module_name(name)}.py": drafting.render_test(
                name, self.request, self.owner, want, named
            ),
        }
        periods = sorted({str(v) for v in values})
        body = self._body(contract, inputs, table, periods, named, rationale, list(texts))
        draft = Draft(
            self.drafts.next_id(f"dp-{contract.table}"),
            f"dp-{contract.table}",
            kind="product",
            title=f"Add data product {name}",
            body=body,
            labels=["data-product"],
            path=f"contracts/{name}.yaml",
            patch=drafting.new_files_patch(texts),
        )
        self.drafts.save(draft)
        self.created.append(draft)
        return ToolResult(
            {
                "draft_id": draft.id,
                "status": draft.status,
                "files": list(texts),
                "rows": table.num_rows,
                "periods": [periods[0], periods[-1]],
                "checks_passed": sorted(named),
            },
            tables=[c.name for c in inputs],
        )

    def _body(
        self,
        c: Contract,
        inputs: list[Contract],
        table: pa.Table,
        periods: list[str],
        checks: dict[str, str],
        rationale: str,
        files: list[str],
    ) -> str:
        floor = max(CLASSIFICATIONS.index(i.classification) for i in inputs)
        sample = table.slice(0, 5).to_pylist()
        lines = [
            "## Request",
            "",
            *(f"> {line}" for line in self.request.strip().splitlines()),
            "",
            f"Requested by {self.owner}, who owns the product once it merges.",
            "",
            "## What this adds",
            "",
            f"`{c.name}`: {c.description}",
            "",
            *(f"- `{f}`" for f in files),
            "",
            "## Governance",
            "",
            f"- Classification **{c.classification}**; its inputs need at least "
            f"{CLASSIFICATIONS[floor]}: "
            + ", ".join(f"`{i.name}` ({i.classification})" for i in inputs)
            + ". Lake Formation tags and Glue tables come from the contract, as for every table.",
            f"- Owner {c.owner}; consumers: {', '.join(c.consumers)}.",
            f"- Grain: one row per {', '.join(c.primary_key)} ({c.transform['grain']}), "
            f"partitioned by `{c.partition_by[0]['column']}`.",
            "- Strict quality gate: "
            + ", ".join(
                f"`{r['rule']}({', '.join(r.get('columns') or [r.get('column') or r.get('sql', '')])})`"
                for r in c.quality
            )
            + ".",
            "- Reads only its declared inputs (checked by the SQL parser), with file access off.",
            "",
            "## Validated at draft time",
            "",
            f"- Built {table.num_rows} rows over {len(periods)} period(s) ({periods[0]} to "
            f"{periods[-1]}) from today's tables; they conform to the contract and pass its rules.",
            *(f"- Check passed: {name}" for name in sorted(checks)),
            "",
            "Sample rows:",
            "",
            drafting.markdown_table(
                c.column_names, [[_jsonable(r[k]) for k in c.column_names] for r in sample]
            ),
            "## Rationale",
            "",
            rationale.strip(),
            "",
            "---",
            "_Drafted by the gridlake self-service data product agent; nothing changes until a "
            "person approves it, and CI runs the drafted tests on the pull request._",
        ]
        return "\n".join(lines)
