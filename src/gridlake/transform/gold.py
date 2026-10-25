"""Silver -> gold data products.

Gold tables are recomputed per affected UTC date and written with a dynamic
partition overwrite, so reruns and backfills are idempotent.
"""

from __future__ import annotations

from datetime import date

import pyarrow as pa

from gridlake.transform.sql import query


def _dates(dates: list[date]) -> pa.Table:
    return pa.table({"d": pa.array(sorted(set(dates)), pa.date32())})


def ba_daily_energy(
    demand: pa.Table, generation: pa.Table, dim_ba: pa.Table, dim_fuel: pa.Table, dates: list[date]
) -> pa.Table:
    return query(
        """
        WITH dem AS (
            SELECT CAST(period AS DATE) AS date, ba_code,
                   sum(demand_mwh) AS total_demand_mwh,
                   max(demand_mwh) AS peak_demand_mwh,
                   CAST(arg_max(hour(period), demand_mwh) AS INTEGER) AS peak_hour_utc,
                   round(avg(abs(forecast_mwh - demand_mwh) / nullif(demand_mwh, 0)) * 100, 2) AS forecast_mape_pct,
                   sum(net_generation_mwh) AS net_generation_mwh,
                   CAST(count(*) AS INTEGER) AS hours_reported
            FROM demand
            WHERE CAST(period AS DATE) IN (SELECT d FROM dates) AND demand_mwh IS NOT NULL
            GROUP BY 1, 2
        ),
        gen AS (
            SELECT CAST(g.period AS DATE) AS date, g.ba_code,
                   sum(greatest(g.generation_mwh, 0)) AS pos_gen,
                   sum(CASE WHEN f.is_renewable THEN greatest(g.generation_mwh, 0) ELSE 0 END) AS ren_gen,
                   sum(greatest(g.generation_mwh, 0) * f.emission_factor_kg_per_mwh) AS emissions_kg
            FROM generation g JOIN dim_fuel f USING (fuel_type)
            WHERE CAST(g.period AS DATE) IN (SELECT d FROM dates)
            GROUP BY 1, 2
        )
        SELECT dem.date, dem.ba_code, b.ba_name, b.region_name, b.interconnection,
               dem.total_demand_mwh, dem.peak_demand_mwh, dem.peak_hour_utc, dem.forecast_mape_pct,
               dem.net_generation_mwh,
               round(100 * gen.ren_gen / nullif(gen.pos_gen, 0), 2) AS renewable_share_pct,
               round(gen.emissions_kg / nullif(gen.pos_gen, 0), 1) AS carbon_intensity_kg_per_mwh,
               round(gen.emissions_kg / 1000, 1) AS total_emissions_t,
               dem.hours_reported
        FROM dem
        JOIN dim_ba b USING (ba_code)
        LEFT JOIN gen USING (date, ba_code)
        ORDER BY dem.date, dem.ba_code
        """,
        demand=demand,
        generation=generation,
        dim_ba=dim_ba,
        dim_fuel=dim_fuel,
        dates=_dates(dates),
    )


def fuel_mix_daily(generation: pa.Table, dim_fuel: pa.Table, dates: list[date]) -> pa.Table:
    return query(
        """
        WITH daily AS (
            SELECT CAST(g.period AS DATE) AS date, g.ba_code, g.fuel_type, f.fuel_category,
                   sum(g.generation_mwh) AS generation_mwh
            FROM generation g JOIN dim_fuel f USING (fuel_type)
            WHERE CAST(g.period AS DATE) IN (SELECT d FROM dates)
            GROUP BY 1, 2, 3, 4
        )
        SELECT date, ba_code, fuel_type, fuel_category, generation_mwh,
               coalesce(round(100 * greatest(generation_mwh, 0)
                     / nullif(sum(greatest(generation_mwh, 0)) OVER (PARTITION BY date, ba_code), 0), 2), 0) AS share_pct
        FROM daily
        ORDER BY date, ba_code, fuel_type
        """,
        generation=generation,
        dim_fuel=dim_fuel,
        dates=_dates(dates),
    )


def demand_forecast_features(demand: pa.Table, dates: list[date]) -> pa.Table:
    """Point-in-time features: every feature is known at least 24h before the target hour."""
    return query(
        """
        WITH base AS (
            SELECT period, ba_code, demand_mwh, forecast_mwh FROM demand WHERE demand_mwh IS NOT NULL
        ),
        feats AS (
            SELECT b.period, b.ba_code,
                   CAST(hour(b.period) AS INTEGER) AS hour_of_day,
                   CAST(isodow(b.period) - 1 AS INTEGER) AS day_of_week,
                   isodow(b.period) >= 6 AS is_weekend,
                   l24.demand_mwh AS demand_lag_24h,
                   l168.demand_mwh AS demand_lag_168h,
                   round(avg(b.demand_mwh) OVER (
                       PARTITION BY b.ba_code ORDER BY b.period
                       RANGE BETWEEN INTERVAL 47 HOUR PRECEDING AND INTERVAL 24 HOUR PRECEDING), 1)
                       AS demand_rolling_24h_mean,
                   b.forecast_mwh AS eia_forecast_mwh,
                   b.demand_mwh AS target_demand_mwh
            FROM base b
            LEFT JOIN base l24  ON l24.ba_code = b.ba_code  AND l24.period  = b.period - INTERVAL 24 HOUR
            LEFT JOIN base l168 ON l168.ba_code = b.ba_code AND l168.period = b.period - INTERVAL 168 HOUR
        )
        SELECT * FROM feats
        WHERE CAST(period AS DATE) IN (SELECT d FROM dates)
        ORDER BY ba_code, period
        """,
        demand=demand,
        dates=_dates(dates),
    )


def grid_reliability_daily(telemetry: pa.Table, dates: list[date]) -> pa.Table:
    return query(
        """
        SELECT CAST(minute_ts AS DATE) AS date, ba_code,
               count(DISTINCT substation_id) AS substations_reporting,
               count(*) AS minutes_observed,
               count(*) FILTER (WHERE min_frequency_hz < 59.95 OR max_frequency_hz > 60.05) AS excursion_minutes,
               min(min_frequency_hz) AS min_frequency_hz,
               max(max_frequency_hz) AS max_frequency_hz,
               max(max_load_mw) AS max_load_mw
        FROM telemetry
        WHERE CAST(minute_ts AS DATE) IN (SELECT d FROM dates)
        GROUP BY 1, 2
        ORDER BY 1, 2
        """,
        telemetry=telemetry,
        dates=_dates(dates),
    )
