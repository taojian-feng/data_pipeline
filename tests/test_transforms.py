from datetime import UTC, date, datetime, timedelta

import pyarrow as pa

from conftest import REPO
from gridlake.ingest.batch import to_bronze_table
from gridlake.transform import gold, silver


def bronze(records, code_key="type", ingest=None):
    t = to_bronze_table(records, code_key, "test", "b1")
    if ingest:
        t = t.set_column(
            t.column_names.index("_ingest_ts"),
            "_ingest_ts",
            pa.array([ingest] * t.num_rows, pa.timestamp("us", tz="UTC")),
        )
    return t


def rec(period, typ, value, ba="CISO"):
    return {
        "period": period,
        "respondent": ba,
        "respondent-name": ba,
        "type": typ,
        "type-name": typ,
        "value": value,
        "value-units": "megawatthours",
    }


def test_demand_pivot_and_latest_restatement_wins():
    t0 = datetime(2026, 10, 2, tzinfo=UTC)
    first = bronze(
        [
            rec("2026-10-01T05", "D", "100"),
            rec("2026-10-01T05", "DF", "90"),
            rec("2026-10-01T05", "NG", "N/A"),
        ],
        ingest=t0,
    )
    restated = bronze([rec("2026-10-01T05", "D", "120")], ingest=t0 + timedelta(days=1))
    full = pa.concat_tables([first, restated])
    out = silver.demand_hourly(full, restated).to_pylist()
    assert len(out) == 1
    row = out[0]
    assert row["period"] == datetime(2026, 10, 1, 5, tzinfo=UTC)
    # restated demand wins, untouched series keep their latest value, malformed -> NULL
    assert (row["demand_mwh"], row["forecast_mwh"], row["net_generation_mwh"]) == (
        120.0,
        90.0,
        None,
    )


def test_telemetry_dedup_is_exactly_once():
    ts = datetime(2026, 10, 1, 0, 0, 10, tzinfo=UTC)
    rows = {
        "event_id": ["a", "a", "b"],
        "substation_id": ["S1"] * 3,
        "ba_code": ["CISO"] * 3,
        "event_ts": [ts, ts, ts + timedelta(seconds=15)],
        "frequency_hz": [60.0, 60.0, 59.9],
        "load_mw": [1.0, 1.0, 2.0],
        "voltage_kv": [230.0] * 3,
        "_partition": [0, 1, 0],
        "_offset": [0, 0, 1],
        "_is_late": [False] * 3,
        "_ingest_ts": [ts] * 3,
    }
    t = pa.table(rows)
    out = silver.grid_telemetry_minute(t, t).to_pylist()
    assert len(out) == 1 and out[0]["event_count"] == 2
    assert out[0]["min_frequency_hz"] == 59.9


def test_late_events_wait_for_reconciliation():
    ts = datetime(2026, 10, 1, 0, 0, 10, tzinfo=UTC)
    t = pa.table(
        {
            "event_id": ["a"],
            "substation_id": ["S1"],
            "ba_code": ["CISO"],
            "event_ts": [ts],
            "frequency_hz": [60.0],
            "load_mw": [1.0],
            "voltage_kv": [230.0],
            "_partition": [0],
            "_offset": [0],
            "_is_late": [True],
            "_ingest_ts": [ts],
        }
    )
    assert silver.grid_telemetry_minute(t, t).num_rows == 0
    assert silver.grid_telemetry_minute(t, t, reconcile=True).num_rows == 1


def hourly_demand(days=9, ba="CISO"):
    start = datetime(2026, 10, 1, tzinfo=UTC)
    periods = [start + timedelta(hours=h) for h in range(24 * days)]
    return pa.table(
        {
            "period": pa.array(periods, pa.timestamp("us", tz="UTC")),
            "ba_code": [ba] * len(periods),
            "demand_mwh": [float(h) for h in range(len(periods))],
            "forecast_mwh": [float(h) + 1 for h in range(len(periods))],
            "net_generation_mwh": [float(h) for h in range(len(periods))],
            "interchange_mwh": [0.0] * len(periods),
        }
    )


def test_forecast_features_are_point_in_time_correct():
    feats = gold.demand_forecast_features(hourly_demand(), [date(2026, 10, 9)]).to_pylist()
    assert len(feats) == 24
    f = feats[0]  # 2026-10-09T00 -> hour index 192
    assert f["target_demand_mwh"] == 192.0
    assert f["demand_lag_24h"] == 168.0 and f["demand_lag_168h"] == 24.0
    # mean of hours 145..168, i.e. the 24h window ending exactly 24h before the target
    assert f["demand_rolling_24h_mean"] == sum(range(145, 169)) / 24
    assert f["day_of_week"] == 4 and f["is_weekend"] is False  # Friday


def test_daily_energy_renewable_share_and_intensity():
    dim_ba = silver.dim_balancing_authority(REPO / "seeds")
    dim_fuel = silver.dim_fuel_type(REPO / "seeds")
    demand = hourly_demand(days=1)
    periods = demand.column("period").to_pylist()
    gen = pa.table(
        {
            "period": pa.array(periods * 2, pa.timestamp("us", tz="UTC")),
            "ba_code": ["CISO"] * 48,
            "fuel_type": ["SUN"] * 24 + ["COL"] * 24,
            "generation_mwh": [30.0] * 24 + [10.0] * 24,
        }
    )
    row = gold.ba_daily_energy(demand, gen, dim_ba, dim_fuel, [date(2026, 10, 1)]).to_pylist()[0]
    assert row["renewable_share_pct"] == 75.0
    assert row["carbon_intensity_kg_per_mwh"] == 250.0  # 25% coal at 1000 kg/MWh
    assert row["peak_hour_utc"] == 23 and row["hours_reported"] == 24
    mix = gold.fuel_mix_daily(gen, dim_fuel, [date(2026, 10, 1)]).to_pylist()
    assert {m["fuel_type"]: m["share_pct"] for m in mix} == {"COL": 25.0, "SUN": 75.0}
