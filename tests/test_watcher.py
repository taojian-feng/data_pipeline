import dataclasses
import json
import random
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pyarrow as pa
import pytest

from conftest import REPO
from gridlake.agent.llm import BedrockClaude
from gridlake.agent.tools import AccessDenied, LocalSql
from gridlake.config import Settings
from gridlake.pipeline import Pipeline
from gridlake.sources.eia import SyntheticEIASource
from gridlake.watcher import AnomalyWatcher, BriefStore, WatcherToolbox, announce, detect, watch
from gridlake.watcher.context import WatchData
from gridlake.watcher.planner import OfflineBriefWriter
from gridlake.watcher.scenarios import GridEventSource

DAY = date(2026, 10, 21)  # a Wednesday
BAS = {
    "PJM": ("PJM Interconnection", "Eastern"),
    "MISO": ("Midcontinent ISO", "Eastern"),
    "NYIS": ("New York ISO", "Eastern"),
    "ERCO": ("ERCOT", "ERCOT"),
}


def _rows(days=21, overrides=None, fuels=None):
    """Steady synthetic gold rows with a little noise, plus per-(ba, date) overrides."""
    daily, mix = [], []
    for ba, (name, ic) in BAS.items():
        rng = random.Random(ba)
        for i in range(days):
            d = DAY - timedelta(days=days - 1 - i)
            weekend = d.weekday() >= 5
            demand = 24 * 1000 * (0.9 if weekend else 1.0) * (1 + rng.gauss(0, 0.005))
            row = {
                "date": d,
                "ba_code": ba,
                "ba_name": name,
                "region_name": ba,
                "interconnection": ic,
                "total_demand_mwh": demand,
                "peak_demand_mwh": demand / 20,
                "peak_hour_utc": 22,
                "forecast_mape_pct": 2.5 + rng.gauss(0, 0.2),
                "net_generation_mwh": demand * 1.01,
                "net_interchange_mwh": demand * 0.01,
                "renewable_share_pct": 25 + rng.gauss(0, 0.5),
                "carbon_intensity_kg_per_mwh": 400 + rng.gauss(0, 3),
                "hours_reported": 24,
            }
            row.update((overrides or {}).get((ba, d), {}))
            daily.append(row)
            shares = (fuels or {}).get((ba, d)) or {
                "NG": 50 + rng.gauss(0, 0.5),
                "WND": 25 + rng.gauss(0, 0.5),
                "COL": 25,
            }
            for fuel, share in shares.items():
                mix.append(
                    {
                        "date": d,
                        "ba_code": ba,
                        "fuel_type": fuel,
                        "fuel_category": "x",
                        "generation_mwh": row["net_generation_mwh"] * share / 100,
                        "share_pct": share,
                    }
                )
    return daily, mix


def _ids(detection):
    return [a.id for a in detection.anomalies]


# -- detection --------------------------------------------------------------------


def test_a_steady_history_has_no_anomalies_even_on_weekends():
    daily, mix = _rows()
    for offset in range(5):  # Wednesday back to Saturday and Sunday
        assert detect(daily, mix, DAY - timedelta(days=offset)).anomalies == []


def test_a_demand_spike_is_flagged_with_its_baseline():
    daily, mix = _rows(overrides={("PJM", DAY): {"total_demand_mwh": 24 * 1150}})
    d = detect(daily, mix, DAY)
    [a] = d.anomalies
    assert a.id == "2026-10-21/PJM/demand" and a.severity == "high" and a.direction == "up"
    assert a.change == pytest.approx(15, abs=1) and a.window.day_type == "weekday"
    assert a.tables == ["gold.ba_daily_energy"]
    assert d.balancing_authorities == ["ERCO", "MISO", "NYIS", "PJM"]


def test_small_moves_are_not_flagged_even_when_statistically_unusual():
    daily, mix = _rows(overrides={("PJM", DAY): {"total_demand_mwh": 24 * 1040}})
    assert detect(daily, mix, DAY).anomalies == []  # z is large, but 4% is under the 5% floor


def test_missing_hours_are_a_data_gap_not_a_demand_drop():
    daily, mix = _rows(
        overrides={("MISO", DAY): {"total_demand_mwh": 21 * 1000, "hours_reported": 21}}
    )
    d = detect(daily, mix, DAY)
    assert d.anomalies == [] and d.data_gaps == {"MISO": 21}
    daily, mix = _rows(
        overrides={("MISO", DAY): {"total_demand_mwh": 10 * 1000, "hours_reported": 10}}
    )
    d = detect(daily, mix, DAY)
    assert d.anomalies == [] and d.data_gaps == {"MISO": 10}  # too few hours to judge at all


def test_interchange_and_fuel_moves_and_new_sources():
    lull = {"NG": 62.0, "WND": 5.0, "COL": 33.0}
    daily, mix = _rows(
        overrides={("ERCO", DAY): {"net_interchange_mwh": -24 * 1000 * 0.08}},
        fuels={
            ("MISO", DAY): lull,
            ("NYIS", DAY): {"NG": 47.0, "WND": 25.0, "COL": 25.0, "BAT": 3.0},
        },
    )
    d = detect(daily, mix, DAY)
    ids = _ids(d)
    assert "2026-10-21/ERCO/interchange" in ids
    assert {"2026-10-21/MISO/fuel_share/WND", "2026-10-21/MISO/fuel_share/NG"} <= set(ids)
    assert "2026-10-21/NYIS/new_source/BAT" in ids
    wind = next(a for a in d.anomalies if a.id.endswith("MISO/fuel_share/WND"))
    assert wind.window.method == "previous_day" and wind.baseline == pytest.approx(25, abs=2)
    assert d.anomalies[0].severity == "high"  # most severe first


def test_too_little_history_is_reported_not_guessed():
    daily, mix = _rows(days=3)
    d = detect(daily, mix, DAY)
    assert d.anomalies == [] and set(d.not_judged) == {"demand", "interchange", "fuel_share"}


# -- context and the brief ------------------------------------------------------------


def _toolbox(contracts, daily, mix, detection, clearance="public"):
    tables = {"gold.ba_daily_energy": pa.Table.from_pylist(daily)}
    return WatcherToolbox(
        contracts, clearance, LocalSql(tables.__getitem__), WatchData(daily, mix, detection)
    )


def test_context_separates_regional_from_local_changes(contracts):
    hot = {"total_demand_mwh": 24 * 1120, "forecast_mape_pct": 11.0}
    regional = {(ba, DAY): hot for ba in ("PJM", "MISO", "NYIS")}
    daily, mix = _rows(overrides=regional)
    d = detect(daily, mix, DAY)
    ctx = _toolbox(contracts, daily, mix, d).call(
        "get_anomaly_context", {"anomaly_id": "2026-10-21/PJM/demand"}
    )
    assert not ctx.is_error and ctx.tables == ["gold.ba_daily_energy", "gold.fuel_mix_daily"]
    hints = " ".join(ctx.content["hints"])
    assert "consistent with a regional driver" in hints and "not anticipated" in hints

    daily, mix = _rows(overrides={("PJM", DAY): {"total_demand_mwh": 24 * 1120}})
    d = detect(daily, mix, DAY)
    ctx = _toolbox(contracts, daily, mix, d).call(
        "get_anomaly_context", {"anomaly_id": "2026-10-21/PJM/demand"}
    )
    hints = " ".join(ctx.content["hints"])
    assert "so the change is local" in hints and "saw it coming" in hints
    bad = _toolbox(contracts, daily, mix, d).call("get_anomaly_context", {"anomaly_id": "nope"})
    assert bad.is_error and "unknown anomaly" in bad.content["error"]


def test_the_offline_brief_explains_each_ba_and_the_table_comes_from_detection(contracts):
    daily, mix = _rows(
        overrides={("ERCO", DAY): {"total_demand_mwh": 24 * 1150, "net_interchange_mwh": -24 * 60}},
        fuels={("MISO", DAY): {"NG": 62.0, "WND": 5.0, "COL": 33.0}},
    )
    d = detect(daily, mix, DAY)
    brief = AnomalyWatcher(OfflineBriefWriter()).brief(d, _toolbox(contracts, daily, mix, d))
    assert [s.tool for s in brief.steps] == ["get_anomaly_context"] * len(d.anomalies)
    assert "**ERCO** (ERCOT): " in brief.narrative
    assert "demand ran 15.1% above a usual weekday" in brief.narrative
    assert "switched from net exporter to net importer" in brief.narrative
    assert "**MISO**" in brief.narrative and "Wind share fell 19.8 points" in brief.narrative
    md = brief.markdown()
    assert md.startswith(f"# Grid brief for 2026-10-21: {len(d.anomalies)} unusual changes")
    assert "| high | ERCO | demand |" in md and "the day before" in md
    assert brief.tables == ["gold.ba_daily_energy", "gold.fuel_mix_daily"]


def test_a_quiet_day_needs_no_model(contracts):
    class NoModel:
        name = "boom"

        def respond(self, *a):
            raise AssertionError("the model must not be called")

    daily, mix = _rows()
    d = detect(daily, mix, DAY)
    brief = AnomalyWatcher(NoModel()).brief(d, _toolbox(contracts, daily, mix, d))
    assert brief.model == "none" and "No unusual" in brief.narrative
    assert "no unusual changes" in brief.markdown()


def test_claude_writes_the_narrative_but_not_the_numbers(contracts):
    daily, mix = _rows(overrides={("PJM", DAY): {"total_demand_mwh": 24 * 1150}})
    d = detect(daily, mix, DAY)
    script = [
        [
            {
                "type": "tool_use",
                "id": "a",
                "name": "get_anomaly_context",
                "input": {"anomaly_id": d.anomalies[0].id},
            }
        ],
        [
            {
                "type": "tool_use",
                "id": "b",
                "name": "run_sql",
                "input": {"sql": "SELECT * FROM gold.grid_reliability_daily"},
            }
        ],
        [{"type": "text", "text": "**PJM** demand ran hot, consistent with a local event."}],
    ]
    seen = []

    class Messages:
        def create(self, **kw):
            seen.append(json.loads(json.dumps(kw, default=str)))
            content = [SimpleNamespace(**b) for b in script[len(seen) - 1]]
            stop = "tool_use" if content[0].type == "tool_use" else "end_turn"
            return SimpleNamespace(stop_reason=stop, content=content)

    llm = BedrockClaude("m", "us-east-2", client=SimpleNamespace(messages=Messages()))
    brief = AnomalyWatcher(llm).brief(d, _toolbox(contracts, daily, mix, d))
    assert seen[0]["system"].startswith("You are gridlake's grid anomaly watcher")
    assert {t["name"] for t in seen[0]["tools"]} == {
        "get_anomaly_context",
        "run_sql",
        "list_tables",
        "describe_table",
        "lookup_entity",
    }
    assert d.anomalies[0].id in seen[0]["messages"][0]["content"]
    # The confidential table is refused under the bound public clearance.
    assert [s.ok for s in brief.steps] == [True, False]
    assert "classified confidential" in brief.steps[1].error
    assert brief.narrative.startswith("**PJM** demand ran hot")
    assert f"| {d.anomalies[0].value:,.1f} |" in brief.markdown()


# -- end to end ----------------------------------------------------------------------


def _settings(home):
    return Settings(
        home=home,
        catalog_type="local",
        eia_api_key=None,
        openlineage_url=None,
        agent_llm="offline",
        agent_sql="local",
        graph_s3_uri=None,
        briefs_s3_uri=None,
        brief_topic_arn=None,
        contracts_dir=REPO / "contracts",
        seeds_dir=REPO / "seeds",
    )


END = datetime(2026, 10, 22, tzinfo=UTC)


@pytest.fixture(scope="module")
def lake_with_events(tmp_path_factory, bas):
    settings = _settings(tmp_path_factory.mktemp("watcher"))
    source = GridEventSource(SyntheticEIASource(bas))
    Pipeline(settings, source=source).run(END - timedelta(days=21), END, produce_telemetry=False)
    return settings


def test_planted_grid_events_are_found_and_explained(lake_with_events):
    brief = watch(lake_with_events, day=date(2026, 10, 21))
    ids = [a.id for a in brief.detection.anomalies]
    assert "2026-10-21/ERCO/demand" in ids
    assert "2026-10-21/SWPP/fuel_share/WND" in ids
    assert "2026-10-21/BPAT/interchange" in ids
    assert {a.ba_code for a in brief.detection.anomalies} == {"ERCO", "SWPP", "BPAT"}
    assert "not anticipated" in brief.narrative and "Hydro output fell" in brief.narrative
    assert brief.model == "offline-brief-writer" and brief.clearance == "public"


def test_the_day_before_the_events_is_quiet(lake_with_events):
    assert watch(lake_with_events, day=date(2026, 10, 20)).detection.anomalies == []


def test_the_watcher_needs_clearance_for_the_gold_tables(lake_with_events, tmp_path):
    import shutil

    shutil.copytree(REPO / "contracts", tmp_path / "contracts")
    path = tmp_path / "contracts/gold.fuel_mix_daily.yaml"
    path.write_text(path.read_text().replace("classification: public", "classification: internal"))
    settings = dataclasses.replace(lake_with_events, contracts_dir=tmp_path / "contracts")
    with pytest.raises(AccessDenied, match=r"gold\.fuel_mix_daily is internal"):
        watch(settings, day=date(2026, 10, 21))
    assert watch(settings, day=date(2026, 10, 21), clearance="internal").detection.anomalies


def test_gold_gains_net_interchange_on_existing_tables(lake_with_events):
    from gridlake.catalog import Lakehouse, get_catalog
    from gridlake.contracts import load_contracts

    lake = Lakehouse(get_catalog(lake_with_events), load_contracts(REPO / "contracts"))
    rows = lake.read("gold.ba_daily_energy").to_pylist()
    bpat = next(r for r in rows if r["ba_code"] == "BPAT" and r["date"] == date(2026, 10, 21))
    assert bpat["net_interchange_mwh"] < 0  # the hydro shortfall turned BPAT into an importer


def test_adding_a_contract_column_evolves_an_existing_table(tmp_path, contracts):
    from gridlake.catalog import Lakehouse, get_catalog

    settings = _settings(tmp_path / "evolve")
    settings.ensure_dirs()
    full = contracts["gold.ba_daily_energy"]
    old = dataclasses.replace(
        full, columns=tuple(c for c in full.columns if c.name != "net_interchange_mwh")
    )
    row = {c.name: None for c in old.columns} | {
        "date": DAY,
        "ba_code": "X",
        "ba_name": "x",
        "region_name": "r",
        "interconnection": "i",
        "total_demand_mwh": 1.0,
        "peak_demand_mwh": 1.0,
        "peak_hour_utc": 1,
        "hours_reported": 24,
    }
    Lakehouse(get_catalog(settings), {old.name: old}).append(old.name, pa.Table.from_pylist([row]))
    lake = Lakehouse(get_catalog(settings), {full.name: full})
    lake.append(
        full.name, pa.Table.from_pylist([row | {"ba_code": "Y", "net_interchange_mwh": 5.0}])
    )
    t = lake.read(full.name)
    assert t.schema.names == full.column_names
    assert sorted(t.select(["ba_code", "net_interchange_mwh"]).to_pylist(), key=str) == [
        {"ba_code": "X", "net_interchange_mwh": None},
        {"ba_code": "Y", "net_interchange_mwh": 5.0},
    ]


def test_briefs_go_to_s3_and_the_topic(lake_with_events):
    brief = watch(lake_with_events, day=date(2026, 10, 21))
    puts, published = {}, []
    s3 = SimpleNamespace(put_object=lambda **kw: puts.setdefault(kw["Key"], kw))
    sns = SimpleNamespace(publish=lambda **kw: published.append(kw))
    where = BriefStore(None, "s3://artifacts/briefs/", client=s3).save(brief)
    assert where == "s3://artifacts/briefs/2026-10-21.md"
    assert set(puts) == {"briefs/2026-10-21.md", "briefs/2026-10-21.json"}
    assert json.loads(puts["briefs/2026-10-21.json"]["Body"])["detection"]["anomalies"]
    assert not announce(lake_with_events, brief, where, sns)  # no topic configured
    settings = dataclasses.replace(lake_with_events, brief_topic_arn="arn:aws:sns:x:1:brief")
    assert announce(settings, brief, where, sns)
    assert published[0]["Subject"].startswith("Grid brief for 2026-10-21: ")
    assert where in published[0]["Message"]


def test_lambda_handler_writes_the_brief(lake_with_events, monkeypatch):
    from gridlake.watcher import aws

    monkeypatch.setattr(aws, "Settings", lambda: lake_with_events)
    out = aws.handler({"date": "2026-10-21"}, None)
    assert out["brief"].endswith("briefs/2026-10-21.md") and not out["announced"]
    assert "2026-10-21/ERCO/demand" in out["anomalies"]
    assert (lake_with_events.briefs_dir / "2026-10-21.json").exists()


def test_cli_watch(tmp_path, monkeypatch, capsys):
    from gridlake.cli import main

    monkeypatch.setenv("GRIDLAKE_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    args = ["run", "--end", "2026-10-15", "--days", "14", "--no-telemetry", "--grid-events"]
    assert main(args) == 0
    capsys.readouterr()
    assert main(["watch", "--date", "2026-10-14", "--trace"]) == 0
    out = capsys.readouterr().out
    assert "> get_anomaly_context(" in out and "**ERCO**" in out
    assert "saved: " in out and (tmp_path / "home/briefs/2026-10-14.md").exists()
    assert main(["watch", "--date", "2026-10-14", "--json", "--no-save"]) == 0
    assert json.loads(capsys.readouterr().out)["detection"]["date"] == "2026-10-14"


def test_unknown_grid_events_are_refused(bas):
    with pytest.raises(ValueError, match="unknown grid events"):
        GridEventSource(SyntheticEIASource(bas), ("meteor",))
