"""DuckDB helpers: run SQL over named Arrow tables, always in UTC."""

from __future__ import annotations

import duckdb
import pyarrow as pa


def connect(**tables: pa.Table) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    for name, table in tables.items():
        con.register(name, table)
    return con


def to_arrow(rel: duckdb.DuckDBPyRelation) -> pa.Table:
    # DuckDB >= 1.4 exposes to_arrow_table(); older releases only have arrow().
    return rel.to_arrow_table() if hasattr(rel, "to_arrow_table") else rel.arrow()


def query(sql: str, **tables: pa.Table) -> pa.Table:
    return to_arrow(connect(**tables).sql(sql))
