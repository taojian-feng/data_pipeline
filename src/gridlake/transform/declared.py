"""Declared data products: gold tables defined by a contract and one SQL file, with no Python.

A contract opts in with a ``transform`` block::

    transform:
      sql: transforms/gold.ba_weekly_energy.sql
      inputs: [gold.ba_daily_energy]
      grain: week          # day | week | month: what the partition date is truncated to

The pipeline builds every declared product after the built-in gold tables, in dependency
order, for the periods whose input dates changed, through the same strict quality gate and
dynamic partition overwrite as hand-written gold. ``check_product`` holds the governance
rules a declared product must meet before it can be built (ADR 0008): it reads only silver
or gold through the inputs it declares, it is classified at least as strictly as every
input, it has an owner, consumers and column descriptions, its grain is unique, and it is
partitioned by its period date.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from gridlake.contracts import CLASSIFICATIONS, Contract
from gridlake.transform.sql import to_arrow

GRAINS = ("day", "week", "month")
SOURCE_LAYERS = ("silver", "gold")
_EMAIL = re.compile(r"^[\w.+-]+@[\w-]+(\.[\w-]+)+$")


class ProductError(ValueError):
    pass


def is_declared(contract: Contract) -> bool:
    return contract.transform is not None


def period_column(contract: Contract) -> str:
    return contract.partition_by[0]["column"] if contract.partition_by else ""


def period_start(day: date, grain: str) -> date:
    """The partition a UTC day falls in; weeks start on Monday, as DuckDB's date_trunc."""
    if grain == "week":
        return day - timedelta(days=day.weekday())
    if grain == "month":
        return day.replace(day=1)
    return day


def _dated(contract: Contract) -> bool:
    types = {c.name: c.type for c in contract.columns}
    return any(types.get(k) in ("date", "timestamp") for k in contract.primary_key)


def strip_comments(sql: str) -> str:
    """The SQL without comments, so a table named in a comment is not mistaken for an input."""
    tokens = duckdb.tokenize(sql)
    out, last = [], 0
    for i, (pos, kind) in enumerate(tokens):
        if kind == duckdb.token_type.comment:
            end = tokens[i + 1][0] if i + 1 < len(tokens) else len(sql)
            out.append(sql[last:pos] + "\n")
            last = end
    return "".join(out) + sql[last:]


def check_product(contract: Contract, contracts: dict[str, Contract], sql: str) -> list[str]:
    """Every governance rule a declared product breaks; an empty list means it may be built."""
    from gridlake.agent.tools import referenced_tables  # the analyst's parser, no cycle at import

    c, spec, out = contract, contract.transform or {}, []
    if c.layer != "gold" or c.namespace != "gold":
        out.append(f"{c.name}: a declared product is a gold table named gold.<name>")
    if spec.get("sql") != f"transforms/{c.name}.sql":
        out.append(f"{c.name}: transform.sql must be transforms/{c.name}.sql")
    if spec.get("grain") not in GRAINS:
        out.append(f"{c.name}: transform.grain must be one of {GRAINS}")
    inputs = list(spec.get("inputs") or [])
    if not inputs:
        out.append(f"{c.name}: transform.inputs must name the tables the SQL reads")
    for name in inputs:
        src = contracts.get(name)
        if src is None:
            out.append(f"{c.name}: input {name} has no contract")
        elif src.name == c.name or src.layer not in SOURCE_LAYERS:
            out.append(f"{c.name}: input {name} must be another silver or gold table")
        elif CLASSIFICATIONS.index(c.classification) < CLASSIFICATIONS.index(src.classification):
            out.append(
                f"{c.name}: classification {c.classification} is below its input {name} "
                f"({src.classification}); a product is at least as restricted as its inputs"
            )
    known = [contracts[i] for i in inputs if i in contracts]
    if known and not any(_dated(s) for s in known):
        out.append(f"{c.name}: at least one input must be dated, or it would never rebuild")
    if not _EMAIL.match(c.owner or ""):
        out.append(f"{c.name}: owner must be a team email address")
    if not c.description:
        out.append(f"{c.name}: a description is required")
    if not c.consumers:
        out.append(f"{c.name}: name at least one consumer")
    undocumented = [col.name for col in c.columns if not col.description]
    if undocumented:
        out.append(f"{c.name}: columns need a description: {undocumented}")
    types = {col.name: col for col in c.columns}
    pcol = period_column(c)
    if (
        len(c.partition_by) != 1
        or c.partition_by[0].get("transform", "identity") != "identity"
        or pcol not in types
        or types[pcol].type != "date"
    ):
        out.append(f"{c.name}: partition by one date column with the identity transform")
    elif pcol not in c.primary_key or types[pcol].nullable:
        out.append(f"{c.name}: the period column {pcol} must be required and in the primary key")
    if not c.primary_key:
        out.append(f"{c.name}: a primary key (the product's grain) is required")
    elif not any(
        r.get("rule") == "unique"
        and set(r.get("columns", [])) == set(c.primary_key)
        and r.get("severity", "error") == "error"
        for r in c.quality
    ):
        out.append(f"{c.name}: add an error-severity unique rule on {list(c.primary_key)}")
    try:
        read = referenced_tables(strip_comments(sql))
    except ValueError as e:
        out.append(f"{c.name}: {e}")
    else:
        extra = sorted(read - set(inputs))
        if extra:
            out.append(f"{c.name}: the SQL reads {extra}, which transform.inputs does not declare")
    return out


def declared_products(contracts: dict[str, Contract]) -> list[Contract]:
    """Declared products in dependency order (a product may read another one)."""
    todo = {n: c for n, c in contracts.items() if is_declared(c)}
    out: list[Contract] = []
    while todo:
        ready = sorted(
            n for n, c in todo.items() if not set(c.transform.get("inputs") or []) & set(todo)
        )
        if not ready:
            raise ProductError(f"declared products depend on each other in a cycle: {sorted(todo)}")
        for n in ready:
            out.append(todo.pop(n))
    return out


def load_sql(contract: Contract, transforms_dir: Path) -> str:
    path = transforms_dir / f"{contract.name}.sql"
    if not path.exists():
        raise ProductError(f"{contract.name}: {path} does not exist")
    return path.read_text()


def query_tables(sql: str, tables: dict[str, pa.Table]) -> pa.Table:
    """Run one SELECT over ``namespace.table`` views of the given Arrow tables, with file
    access off and locked, so a product's SQL can read nothing it did not declare."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    for name, table in sorted(tables.items()):
        ns, tbl = name.split(".")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {ns}")
        con.register(f"_{ns}_{tbl}", table)
        con.execute(f'CREATE VIEW {ns}.{tbl} AS SELECT * FROM "_{ns}_{tbl}"')
    con.execute("SET enable_external_access = false")
    con.execute("SET lock_configuration = true")
    return to_arrow(con.sql(sql))


def build(
    contract: Contract,
    sql: str,
    read: Callable[[str], pa.Table],
    days: list[date],
    contracts: dict[str, Contract] | None = None,
) -> tuple[pa.Table, list[date]]:
    """Rows of the periods covering ``days``, and those periods (the partitions to overwrite).

    The SQL runs over its full inputs, so windows and period totals see complete history,
    and the result is narrowed to the affected periods."""
    if contracts is not None:
        problems = check_product(contract, contracts, sql)
        if problems:
            raise ProductError("; ".join(problems))
    grain, col = contract.transform["grain"], period_column(contract)
    periods = sorted({period_start(d, grain) for d in days})
    data = query_tables(sql, {i: read(i) for i in contract.transform["inputs"]})
    if col not in data.column_names:
        raise ProductError(f"{contract.name}: the SQL does not return the period column {col}")
    values = data.column(col).cast(pa.date32())
    misaligned = {v for v in values.to_pylist() if v is not None and period_start(v, grain) != v}
    if misaligned:
        raise ProductError(
            f"{contract.name}: {col} must be the start of each {grain}; got {sorted(misaligned)[:3]}"
        )
    keep = pc.is_in(values, pa.array(periods, pa.date32()))
    return data.filter(keep), periods


def run_checks(checks: dict[str, str], tables: dict[str, pa.Table]) -> dict[str, int]:
    """Singular tests: each check is a SELECT returning the rows that violate it."""
    return {name: query_tables(sql, tables).num_rows for name, sql in sorted(checks.items())}


# -- the harness generated tests call -----------------------------------------------


@dataclass
class ProductReport:
    table: str
    rows: int
    columns: list[str]
    periods: list[str]
    governance: list[str] = field(default_factory=list)
    quality_failures: list[str] = field(default_factory=list)
    check_failures: dict[str, int] = field(default_factory=dict)
    idempotent: bool = False


def verify_product(
    table: str,
    home: Path,
    repo_root: Path,
    checks: dict[str, str] | None = None,
    start: date = date(2026, 10, 1),
    days: int = 21,
) -> ProductReport:
    """Build one declared product end to end on synthetic data in ``home`` and report on it:
    governance, the contract's quality rules, the product's own checks, and whether a rebuild
    changes nothing. The tests the self-service agent drafts are thin wrappers over this."""
    from gridlake.config import Settings
    from gridlake.contracts import load_contracts
    from gridlake.pipeline import Pipeline
    from gridlake.quality.engine import QualityGateError, evaluate
    from gridlake.sources.eia import SyntheticEIASource, load_balancing_authorities

    settings = Settings(
        home=home,
        catalog_type="local",
        eia_api_key=None,
        openlineage_url=None,
        kb_s3_uri=None,
        graph_s3_uri=None,
        contracts_dir=repo_root / "contracts",
        seeds_dir=repo_root / "seeds",
        transforms_dir=repo_root / "transforms",
    )
    contracts = load_contracts(settings.contracts_dir)
    contract = contracts[table]
    sql = load_sql(contract, settings.transforms_dir)
    report = ProductReport(table, 0, [], [], governance=check_product(contract, contracts, sql))
    if report.governance:
        return report
    needs_telemetry = _reads_telemetry(contract, contracts)
    begin = datetime.combine(start, datetime.min.time(), UTC)
    end = begin + timedelta(days=days)
    pipe = Pipeline(
        settings, source=SyntheticEIASource(load_balancing_authorities(settings.seeds_dir))
    )
    pipe.seed_dimensions()
    pipe.ingest_eia(begin, end)
    if needs_telemetry:
        # Telemetry is simulated per minute, so a few hours on each of two days is plenty.
        for d in (begin, end - timedelta(days=1)):
            pipe.ingest_stream(d, d + timedelta(hours=2))
    affected = pipe.build_silver()
    pipe.build_gold(affected)
    first = pipe.lake.read(table)
    report.rows, report.columns = first.num_rows, first.column_names
    report.periods = sorted({str(v) for v in first.column(period_column(contract)).to_pylist()})
    try:
        evaluate(contract, first, mode="strict")
    except QualityGateError as e:
        report.quality_failures = [r.rule for r in e.report.results if not r.passed]
    tables = {i: pipe.lake.read(i) for i in contract.transform["inputs"]} | {table: first}
    report.check_failures = run_checks(checks or {}, tables)
    # Rebuild every period from scratch: the table must come out the same.
    pipe.build_products({i: _all_days(pipe, i) for i in contract.transform["inputs"]})
    second = pipe.lake.read(table)
    report.idempotent = _rows(first) == _rows(second)
    return report


def _reads_telemetry(contract: Contract, contracts: dict[str, Contract]) -> bool:
    seen, todo = set(), list(contract.transform["inputs"])
    while todo:
        name = todo.pop()
        if name in seen:
            continue
        seen.add(name)
        c = contracts.get(name)
        if c is not None and c.transform:
            todo += list(c.transform.get("inputs") or [])
    return bool(seen & {"silver.grid_telemetry_minute", "gold.grid_reliability_daily"})


def _all_days(pipe: Any, table: str) -> list[date]:
    c = pipe.contracts[table]
    col = next(
        (
            k
            for k in c.primary_key
            if c.columns[c.column_names.index(k)].type in ("date", "timestamp")
        ),
        None,
    )
    if col is None:
        return []
    out = set()
    for v in pipe.lake.read(table).column(col).to_pylist():
        out.add(v.date() if isinstance(v, datetime) else v)
    return sorted(out)


def _rows(t: pa.Table) -> list[str]:
    return sorted(json.dumps(r, default=str, sort_keys=True) for r in t.to_pylist())
