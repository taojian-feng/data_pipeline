"""End-to-end: run the whole pipeline against a throwaway local lakehouse."""

from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest

from conftest import START
from gridlake import lineage
from gridlake.pipeline import Pipeline
from gridlake.quality.engine import QualityGateError
from gridlake.sources.eia import SyntheticEIASource

END = START + timedelta(days=9)


@pytest.fixture
def first_run(settings):
    pipe = Pipeline(settings)
    summary = pipe.run(START, END, telemetry_hours=2)
    return pipe, summary


def counts(pipe):
    return {
        name: pipe.lake.read(name).num_rows
        for name in pipe.contracts
        if not name.startswith("quality.")
    }


def test_end_to_end_run_publishes_every_product(first_run):
    pipe, summary = first_run
    assert summary.ok
    n = counts(pipe)
    ba = len(pipe.bas)
    assert n["silver.dim_balancing_authority"] == ba
    assert n["gold.ba_daily_energy"] == ba * 9
    assert n["gold.fuel_mix_daily"] > 0 and n["gold.grid_reliability_daily"] > 0
    # defects were quarantined, not published
    assert n["silver.demand_hourly"] < ba * 24 * 9
    assert pipe.lake.read("quality.quarantine").num_rows > 0
    assert (pipe.settings.knowledge_dir / "graph.ttl").exists()
    assert any(pipe.settings.knowledge_dir.joinpath("bedrock_kb").iterdir())


def test_rerun_is_idempotent(first_run, settings):
    pipe, _ = first_run
    before = counts(pipe)
    snapshots = {
        t: pipe.lake.snapshot_id(t) for t in ("silver.demand_hourly", "gold.ba_daily_energy")
    }
    again = Pipeline(settings).run(START, END, telemetry_hours=2, produce_telemetry=False)
    assert again.ok
    assert counts(pipe) == before
    ingest = next(s for s in again.steps if s.name == "ingest_eia").metrics
    assert all(v["skipped"] for v in ingest.values())
    assert pipe.lake.snapshot_id("silver.demand_hourly") == snapshots["silver.demand_hourly"]


def test_redelivered_stream_events_do_not_double_count(first_run, settings):
    pipe, _ = first_run
    events = pipe.lake.read("silver.grid_telemetry_minute").column("event_count").to_pylist()
    # Producers replay the same window: at-least-once in, exactly-once out.
    Pipeline(settings).run(START, END, telemetry_hours=2, produce_telemetry=True)
    assert (
        pipe.lake.read("silver.grid_telemetry_minute").column("event_count").to_pylist() == events
    )


class RestatingSource(SyntheticEIASource):
    """EIA revises recent hours: same window, CISO demand restated +10%."""

    name = "synthetic-eia930"

    def region_data(self, start, end):
        out = super().region_data(start, end)
        for r in out:
            if r["respondent"] == "CISO" and r["type"] == "D" and r["value"] not in (None, "N/A"):
                r["value"] = str(round(float(r["value"]) * 1.1))
        return out


def test_restatement_flows_through_to_gold(first_run, settings):
    pipe, _ = first_run

    def sql():
        return {
            r["date"]: r["total_demand_mwh"]
            for r in pipe.lake.read(
                "gold.ba_daily_energy", row_filter="ba_code == 'CISO'"
            ).to_pylist()
        }

    before = sql()
    Pipeline(settings, source=RestatingSource(pipe.bas)).run(START, END, produce_telemetry=False)
    after = sql()
    assert after.keys() == before.keys()
    assert all(after[d] > before[d] * 1.05 for d in before)
    others = pipe.lake.read("gold.ba_daily_energy", row_filter="ba_code == 'PJM'").num_rows
    assert others == 9  # untouched BAs keep their rows (partition overwrite, not truncate)


def test_quality_gate_stops_publication(settings, monkeypatch):
    from gridlake.transform import gold

    real = gold.ba_daily_energy

    def broken(*args, **kwargs):  # a bug that duplicates a row
        t = real(*args, **kwargs)
        return pa.concat_tables([t, t.slice(0, 1)])

    monkeypatch.setattr(gold, "ba_daily_energy", broken)
    pipe = Pipeline(settings)
    with pytest.raises(QualityGateError, match="unique"):
        pipe.run(START, START + timedelta(days=2), telemetry_hours=1)
    assert pipe.lake.read("gold.ba_daily_energy").num_rows == 0  # nothing published
    failed = [s for s in pipe.steps if s.status == "FAIL"]
    assert [s.name for s in failed] == ["gold_ba_daily_energy"]
    events = [
        e["eventType"] for e in pipe.lineage.events if e["job"]["name"] == "gold_ba_daily_energy"
    ]
    assert events == ["START", "FAIL"]


def test_lineage_graph_is_complete(first_run):
    pipe, _ = first_run
    edges = {(s, d) for s, _, d in lineage.read_edges(pipe.settings.lineage_dir)}
    assert ("bronze.eia_region_data", "silver.demand_hourly") in edges
    assert ("silver.demand_hourly", "gold.demand_forecast_features") in edges
    assert ("silver.grid_telemetry_minute", "gold.grid_reliability_daily") in edges
    complete = [e for e in pipe.lineage.events if e["eventType"] == "COMPLETE" and e["outputs"]]
    out = complete[-1]["outputs"][0]
    assert {"schema", "ownership", "version"} <= out["facets"].keys()


def test_step_mode_matches_full_run(first_run, tmp_path):
    from gridlake.config import Settings

    full, _ = first_run
    stepped = Pipeline(Settings(home=tmp_path / "stepped", eia_api_key=None, openlineage_url=None))
    t0 = datetime.now(UTC)
    for step in Pipeline.STEPS:
        assert stepped.run_step(step, START, END, since=t0, telemetry_hours=2).ok
    for table in ("gold.ba_daily_energy", "gold.fuel_mix_daily", "gold.demand_forecast_features"):
        assert stepped.lake.read(table).num_rows == full.lake.read(table).num_rows, table


def test_partition_spec_is_evolved_onto_precreated_tables(settings):
    pipe = Pipeline(settings)
    c = pipe.contracts["silver.demand_hourly"]
    pipe.lake.catalog.create_namespace_if_not_exists("silver")
    pipe.lake.catalog.create_table(c.name, schema=c.iceberg_schema())  # e.g. created by Terraform
    spec = pipe.lake.table(c.name).spec()
    assert [f.name for f in spec.fields] == ["period_day"]


def test_kinesis_lambda_record_decoding():
    import base64
    import json

    from gridlake.aws_lambda import kinesis_records_to_table

    def rec(seq, ts):
        body = {
            "event_id": seq,
            "substation_id": "CISO-SS01",
            "ba_code": "CISO",
            "event_ts": ts,
            "frequency_hz": 60.0,
            "load_mw": 1.0,
            "voltage_kv": 230.0,
        }
        return {
            "eventID": f"shardId-000000000003:{seq}",
            "kinesis": {
                "sequenceNumber": seq,
                "data": base64.b64encode(json.dumps(body).encode()).decode(),
            },
        }

    t = kinesis_records_to_table(
        [rec("1", "2026-10-01T01:00:00+00:00"), rec("2", "2026-10-01T00:00:00+00:00")]
    )
    assert t.column("_partition").to_pylist() == ["shardId-000000000003"] * 2
    assert t.column("_is_late").to_pylist() == [False, True]
