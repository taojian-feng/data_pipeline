"""Idempotent batch landing of API extracts into bronze Iceberg tables.

Each extract gets a content-addressed ``_batch_id``. Re-running the same window is a
no-op, while a restated extract (EIA revises recent hours) lands as a new batch and
the newer values win in silver.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import pyarrow as pa
from pyiceberg.expressions import EqualTo

from gridlake.catalog import Lakehouse


def batch_id(records: list[dict], source: str) -> str:
    h = hashlib.sha256(source.encode())
    for r in records:
        h.update(json.dumps(r, sort_keys=True).encode())
    return h.hexdigest()[:24]


def to_bronze_table(records: list[dict], code_key: str, source: str, bid: str) -> pa.Table:
    now = datetime.now(UTC)
    return pa.table(
        {
            "period": [r.get("period") for r in records],
            "respondent": [r.get("respondent") for r in records],
            "respondent_name": [r.get("respondent-name") for r in records],
            code_key: [r.get(code_key) for r in records],
            "type_name": [r.get("type-name") for r in records],
            "value": [None if r.get("value") is None else str(r["value"]) for r in records],
            "value_units": [r.get("value-units") for r in records],
            "_batch_id": [bid] * len(records),
            "_ingest_ts": pa.array([now] * len(records), pa.timestamp("us", tz="UTC")),
            "_source": [source] * len(records),
        }
    )


def land(lake: Lakehouse, table: str, records: list[dict], code_key: str, source: str) -> dict:
    bid = batch_id(records, source)
    existing = lake.table(table).scan(
        row_filter=EqualTo("_batch_id", bid), selected_fields=("_batch_id",), limit=1
    )
    if existing.to_arrow().num_rows:
        return {"table": table, "batch_id": bid, "rows": 0, "skipped": True}
    n = lake.append(table, to_bronze_table(records, code_key, source, bid))
    return {"table": table, "batch_id": bid, "rows": n, "skipped": False}
