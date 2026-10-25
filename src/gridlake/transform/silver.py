"""Bronze -> silver: type, validate, deduplicate and conform.

Silver tables are maintained incrementally. Each run reads only bronze rows ingested
after the high-water mark stored on the silver table, finds the business keys they
touch, recomputes those keys from the full bronze history (latest ingest wins, so EIA
restatements are applied correctly), and MERGEs the result on the primary key.
"""

from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa

from gridlake.transform.sql import query

_TS = "timezone('UTC', strptime(period, '%Y-%m-%dT%H'))"


def dim_balancing_authority(seeds_dir: Path) -> pa.Table:
    with (seeds_dir / "balancing_authorities.csv").open() as f:
        rows = [
            {
                k: r[k]
                for k in (
                    "ba_code",
                    "ba_name",
                    "region_code",
                    "region_name",
                    "interconnection",
                    "timezone",
                    "aliases",
                )
            }
            for r in csv.DictReader(f)
        ]
    return pa.Table.from_pylist(rows)


def dim_fuel_type(seeds_dir: Path) -> pa.Table:
    with (seeds_dir / "fuel_types.csv").open() as f:
        rows = [
            {
                **r,
                "is_renewable": r["is_renewable"] == "true",
                "emission_factor_kg_per_mwh": float(r["emission_factor_kg_per_mwh"]),
            }
            for r in csv.DictReader(f)
        ]
    return pa.Table.from_pylist(rows)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def demand_hourly(bronze: pa.Table, new_rows: pa.Table) -> pa.Table:
    """Pivot EIA region-data series (D, DF, NG, TI) into one row per (period, ba_code)."""
    return query(
        f"""
        WITH touched AS (SELECT DISTINCT period, respondent FROM new_rows),
        latest AS (
            SELECT b.* FROM bronze b JOIN touched USING (period, respondent)
            QUALIFY row_number() OVER (PARTITION BY b.period, b.respondent, b.type ORDER BY b._ingest_ts DESC) = 1
        )
        SELECT {_TS} AS period,
               respondent AS ba_code,
               max(CASE WHEN type = 'D'  THEN TRY_CAST(value AS DOUBLE) END) AS demand_mwh,
               max(CASE WHEN type = 'DF' THEN TRY_CAST(value AS DOUBLE) END) AS forecast_mwh,
               max(CASE WHEN type = 'NG' THEN TRY_CAST(value AS DOUBLE) END) AS net_generation_mwh,
               max(CASE WHEN type = 'TI' THEN TRY_CAST(value AS DOUBLE) END) AS interchange_mwh,
               '{_now_iso()}'::TIMESTAMPTZ AS _updated_ts
        FROM latest
        GROUP BY 1, 2
        ORDER BY ba_code, period
        """,
        bronze=bronze,
        new_rows=new_rows,
    )


def generation_hourly(bronze: pa.Table, new_rows: pa.Table) -> pa.Table:
    return query(
        f"""
        WITH touched AS (SELECT DISTINCT period, respondent, fueltype FROM new_rows),
        latest AS (
            SELECT b.* FROM bronze b JOIN touched USING (period, respondent, fueltype)
            QUALIFY row_number() OVER (PARTITION BY b.period, b.respondent, b.fueltype ORDER BY b._ingest_ts DESC) = 1
        )
        SELECT {_TS} AS period,
               respondent AS ba_code,
               fueltype AS fuel_type,
               TRY_CAST(value AS DOUBLE) AS generation_mwh,
               '{_now_iso()}'::TIMESTAMPTZ AS _updated_ts
        FROM latest
        ORDER BY ba_code, fuel_type, period
        """,
        bronze=bronze,
        new_rows=new_rows,
    )


def grid_telemetry_minute(
    bronze: pa.Table, new_rows: pa.Table, reconcile: bool = False
) -> pa.Table:
    """Deduplicate stream events on event_id and aggregate to substation-minutes.

    On the incremental path, minutes are recomputed only for on-time events; late
    events (behind the watermark) are folded in by the ``reconcile`` pass, which
    recomputes every minute any new event touched.
    """
    late_filter = "" if reconcile else "WHERE NOT _is_late"
    return query(
        f"""
        WITH touched AS (
            SELECT DISTINCT date_trunc('minute', event_ts) AS minute_ts, substation_id FROM new_rows {late_filter}
        ),
        dedup AS (
            SELECT * FROM bronze
            QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY _ingest_ts, _partition, _offset) = 1
        )
        SELECT date_trunc('minute', d.event_ts) AS minute_ts,
               d.substation_id,
               any_value(d.ba_code) AS ba_code,
               round(avg(d.frequency_hz), 4) AS avg_frequency_hz,
               min(d.frequency_hz) AS min_frequency_hz,
               max(d.frequency_hz) AS max_frequency_hz,
               max(d.load_mw) AS max_load_mw,
               round(avg(d.voltage_kv), 2) AS avg_voltage_kv,
               count(*) AS event_count,
               '{_now_iso()}'::TIMESTAMPTZ AS _updated_ts
        FROM dedup d
        JOIN touched t ON t.minute_ts = date_trunc('minute', d.event_ts) AND t.substation_id = d.substation_id
        GROUP BY 1, 2
        ORDER BY 2, 1
        """,
        bronze=bronze,
        new_rows=new_rows,
    )
