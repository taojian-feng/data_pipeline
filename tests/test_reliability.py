"""The data reliability agent: detection, triage of realistic faults, drafts and approval."""

import contextlib
import json
import shutil
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pyarrow as pa
import pytest

from conftest import REPO, START
from gridlake.agent.tools import LocalSql
from gridlake.config import Settings
from gridlake.contracts import load_contracts
from gridlake.pipeline import Pipeline
from gridlake.reliability import (
    DraftStore,
    Incident,
    LocalRunLog,
    ReliabilityToolbox,
    RunRecord,
    StepFunctionsRunLog,
    detect_incidents,
    quality_for_run,
    triage_run,
)
from gridlake.reliability.drafts import GitHubIssues, approve, reject
from gridlake.reliability.faults import FaultySource
from gridlake.reliability.planner import OfflineResponder
from gridlake.reliability.runlog import RunStep
from gridlake.sources.eia import SyntheticEIASource

END = START + timedelta(days=5)


def _settings(tmp_path, name, contracts_dir=REPO / "contracts", seeds_dir=REPO / "seeds"):
    return Settings(
        home=tmp_path / name,
        catalog_type="local",
        eia_api_key=None,
        openlineage_url=None,
        agent_llm="offline",
        agent_sql="local",
        incidents_s3_uri=None,
        state_machine_arn=None,
        graph_s3_uri=None,
        contracts_dir=contracts_dir,
        seeds_dir=seeds_dir,
    )


def _run(settings, bas, fault=None):
    source = SyntheticEIASource(bas)
    pipe = Pipeline(settings, source=FaultySource(source, fault) if fault else source)
    # A failing run still writes its manifest, as the scheduler would see it.
    with contextlib.suppress(Exception):
        pipe.run(START, END, telemetry_hours=1)
    return pipe


@pytest.fixture(scope="module")
def runs(tmp_path_factory, bas):
    base = tmp_path_factory.mktemp("reliability")
    out = {}
    for fault in (None, "unit_change", "new_fuel_type", "feed_outage"):
        settings = _settings(base, fault or "baseline")
        _run(settings, bas, fault)
        out[fault or "baseline"] = settings
    return out


# -- detection ------------------------------------------------------------------


def test_a_clean_run_has_no_incidents(runs):
    run, triages = triage_run(runs["baseline"])
    assert run.status == "SUCCEEDED" and triages == []


def test_detection_ignores_the_background_defect_rate_but_not_a_spike():
    run = RunRecord("r1", "2026-10-01T00:00:00+00:00", "SUCCEEDED", [], "local", None, ["r1"])
    rows = [
        ("silver.demand_hourly", "not_null(demand_mwh)", "error", 6, 1000),  # 0.6%: background
        ("silver.demand_hourly", "range(demand_mwh:0..250000)", "error", 80, 1000),  # spike
        ("gold.demand_forecast_features", "not_null(demand_lag_24h)", "warn", 200, 1000),
        ("silver.generation_hourly", "unique(period)", "error", 3, 10),  # under min_rows
    ]
    quality = pa.Table.from_pylist(
        [
            {
                "run_id": "r1",
                "table_name": t,
                "rule": r,
                "severity": s,
                "failed_rows": f,
                "total_rows": n,
            }
            for t, r, s, f, n in rows
        ]
    )
    [inc] = detect_incidents(run, quality)
    assert (inc.kind, inc.rule, inc.failed_rows) == (
        "quality_spike",
        "range(demand_mwh:0..250000)",
        80,
    )
    assert inc.id == "inc-r1-1" and inc.failure_rate == 0.08


def test_a_failed_gate_is_one_incident_with_its_table_and_rule():
    error = "QualityGateError: quality gate failed for gold.ba_daily_energy: unique(date,ba_code) (3 rows)"
    run = RunRecord(
        "r2",
        "2026-10-01T00:00:00+00:00",
        "FAILED",
        [RunStep("gold_ba_daily_energy", "FAIL", error=error)],
        "local",
        None,
        ["r2"],
    )
    quality = pa.Table.from_pylist(
        [
            {
                "run_id": "r2",
                "table_name": "gold.ba_daily_energy",
                "rule": "unique(date,ba_code)",
                "severity": "error",
                "failed_rows": 3,
                "total_rows": 10,
            }
        ]
    )
    [inc] = detect_incidents(run, quality)
    assert (inc.kind, inc.table, inc.rule, inc.failed_rows) == (
        "step_failed",
        "gold.ba_daily_energy",
        "unique(date,ba_code)",
        3,
    )


# -- triage of realistic faults --------------------------------------------------


def test_unit_change_is_a_source_defect_and_never_a_contract_change(runs):
    _, [t] = triage_run(runs["unit_change"])
    assert t.incident.table == "silver.demand_hourly"
    assert t.diagnosis["category"] == "source_data_defect"
    assert t.diagnosis["confidence"] == "high"
    assert t.diagnosis["owner"] == "data-platform@gridlake.example"  # the bronze source owner
    assert "ERCO" in t.diagnosis["cause"] and "1,000x" in t.diagnosis["cause"]
    [draft] = t.drafts
    assert draft.kind == "issue" and draft.status == "pending" and draft.patch is None
    assert "do not relax the rule" in draft.body
    assert "gold.ba_daily_energy" in draft.body  # downstream impact
    tools = [s.tool for s in t.steps]
    assert tools[:5] == [
        "get_run_log",
        "get_quality_results",
        "get_lineage",
        "get_contract",
        "inspect_quarantine",
    ]
    assert tools.index("record_diagnosis") < tools.index("draft_issue")


def test_feed_outage_is_an_upstream_outage(runs):
    run, [t] = triage_run(runs["feed_outage"])
    assert run.status == "FAILED"
    assert t.incident.kind == "step_failed" and t.incident.step == "ingest_eia"
    assert "RuntimeError" in t.incident.error  # from the OpenLineage FAIL event
    assert t.diagnosis["category"] == "upstream_outage"
    assert "re-run the same window" in t.drafts[0].body


def test_new_fuel_type_fix_applies_and_replay_restores_the_rows(runs, tmp_path, bas):
    # Work on a copy of the contracts and seeds so the approved patch can be applied.
    repo = tmp_path / "repo"
    shutil.copytree(REPO / "contracts", repo / "contracts")
    shutil.copytree(REPO / "seeds", repo / "seeds")
    settings = replace(
        runs["new_fuel_type"], contracts_dir=repo / "contracts", seeds_dir=repo / "seeds"
    )

    _, [t] = triage_run(settings)
    assert t.diagnosis["category"] == "reference_data_gap"
    [draft] = t.drafts
    assert draft.kind == "fix" and draft.path == "seeds/fuel_types.csv"
    assert "+BAT,Battery Storage,Storage,false,0" in draft.patch
    assert not (repo / "seeds/fuel_types.csv").read_text().count("BAT")  # nothing applied yet

    store = DraftStore(settings.incidents_dir)
    done = approve(store, draft.id, "taojian", tmp_path / "approved", repo, apply=True)
    assert done.status == "approved" and done.decided_by == "taojian"
    assert "BAT,Battery Storage" in (repo / "seeds/fuel_types.csv").read_text()
    with pytest.raises(ValueError, match="already approved"):
        approve(store, draft.id, "taojian", tmp_path / "approved", repo)

    pipe = Pipeline(settings, source=FaultySource(SyntheticEIASource(bas), "new_fuel_type"))
    pipe.run(START, END, telemetry_hours=1, produce_telemetry=False)  # reloads the seed
    metrics = pipe.replay_quarantine("silver.generation_hourly", "referential(fuel_type")
    assert metrics["rows"] == metrics["replayed"] > 0
    gen = pipe.lake.read("silver.generation_hourly").to_pylist()
    assert sum(r["fuel_type"] == "BAT" for r in gen) == metrics["rows"]
    mix = pipe.lake.read("gold.fuel_mix_daily").to_pylist()
    assert {r["ba_code"] for r in mix if r["fuel_type"] == "BAT"} == {"CISO", "ERCO"}


# -- the gold gate, guardrails and access -----------------------------------------


def _toolbox(settings, incident, run, clearance="internal"):
    from gridlake.catalog import Lakehouse, get_catalog
    from gridlake.lineage import read_edges

    contracts = load_contracts(settings.contracts_dir)
    lake = Lakehouse(get_catalog(settings), contracts)
    return ReliabilityToolbox(
        contracts,
        clearance,
        LocalSql(lake.read),
        lake.read,
        incident,
        run,
        LocalRunLog(settings.runs_dir, settings.lineage_dir),
        DraftStore(settings.incidents_dir),
        settings.contracts_dir,
        settings.seeds_dir,
        lambda: read_edges(settings.lineage_dir),
    )


@pytest.fixture
def baseline_box(runs):
    settings = runs["baseline"]
    run = LocalRunLog(settings.runs_dir, settings.lineage_dir).latest()
    inc = Incident(
        "inc-test-1",
        "quality_spike",
        run.run_id,
        run.started,
        table="silver.demand_hourly",
        rule="not_null(demand_mwh)",
    )
    return _toolbox(settings, inc, run)


def test_gold_gate_failure_with_clean_inputs_is_a_transformation_bug(runs):
    from gridlake.reliability.agent import ReliabilityAgent

    settings = runs["baseline"]
    run = LocalRunLog(settings.runs_dir, settings.lineage_dir).latest()
    error = "QualityGateError: quality gate failed for gold.ba_daily_energy: unique(date,ba_code) (3 rows)"
    run = replace(
        run,
        status="FAILED",
        steps=[*run.steps, RunStep("gold_ba_daily_energy", "FAIL", error=error)],
    )
    [inc] = detect_incidents(run, pa.table({}))
    t = ReliabilityAgent(OfflineResponder()).triage(inc, _toolbox(settings, inc, run))
    assert t.diagnosis["category"] == "transformation_bug" and t.diagnosis["confidence"] == "high"
    assert "Do not relax the gold rule" in t.drafts[0].body


def test_drafting_needs_a_diagnosis_first(baseline_box):
    r = baseline_box.call("draft_issue", {"title": "t", "body": "b"})
    assert r.is_error and "record_diagnosis" in r.content["error"]


def test_fixes_are_confined_to_contracts_and_seeds(baseline_box):
    box = baseline_box
    box.call(
        "record_diagnosis",
        {"category": "unknown", "cause": "c", "evidence": ["e"], "confidence": "low"},
    )
    for path in (
        "src/gridlake/cli.py",
        "contracts/../src/x.yaml",
        "/etc/passwd",
        ".github/workflows/ci.yml",
    ):
        r = box.call(
            "draft_fix",
            {"path": path, "edits": [{"old": "a", "new": "b"}], "title": "t", "rationale": "r"},
        )
        assert r.is_error and "access denied" in r.content["error"]


def test_contract_fixes_must_bump_the_version_and_never_loosen_gold(baseline_box):
    box = baseline_box
    box.call(
        "record_diagnosis",
        {"category": "contract_out_of_date", "cause": "c", "evidence": ["e"], "confidence": "low"},
    )
    loosen = {"old": "max: 250000, severity: error", "new": "max: 500000, severity: error"}
    r = box.call(
        "draft_fix",
        {
            "path": "contracts/silver.demand_hourly.yaml",
            "edits": [loosen],
            "title": "t",
            "rationale": "r",
        },
    )
    assert r.is_error and "bump the contract version" in r.content["error"]
    bump = {"old": "version: 1.0.0\n", "new": "version: 1.1.0\n"}
    r = box.call(
        "draft_fix",
        {
            "path": "contracts/silver.demand_hourly.yaml",
            "edits": [loosen, bump],
            "title": "t",
            "rationale": "r",
        },
    )
    assert (
        not r.is_error
        and "+  - {rule: range, column: demand_mwh, min: 0, max: 500000" in r.content["patch"]
    )
    gold = (REPO / "contracts/gold.ba_daily_energy.yaml").read_text()
    rule_line = next(line for line in gold.splitlines() if "rule: range" in line)
    version = next(line for line in gold.splitlines() if line.startswith("version:")).split()[1]
    r = box.call(
        "draft_fix",
        {
            "path": "contracts/gold.ba_daily_energy.yaml",
            "edits": [
                {"old": rule_line, "new": rule_line.replace("severity: error", "severity: warn")},
                {"old": f"version: {version}\n", "new": "version: 9.0.0\n"},
            ],
            "title": "t",
            "rationale": "r",
        },
    )
    assert r.is_error and "gold quality rules are not changed" in r.content["error"]


def test_seed_fixes_must_pass_the_dimension_rules(baseline_box):
    box = baseline_box
    box.call(
        "record_diagnosis",
        {"category": "reference_data_gap", "cause": "c", "evidence": ["e"], "confidence": "low"},
    )
    last = "OTH,Other,Other,false,300\n"
    for new, why in (
        (last + "BAT,Battery Storage,Storage,false,5000\n", "silver.dim_fuel_type would reject"),
        (last + "OTH,Duplicate,Other,false,300\n", "silver.dim_fuel_type would reject"),
        (last + "BAT,Battery Storage\n", "do not have 5 fields"),
    ):
        r = box.call(
            "draft_fix",
            {
                "path": "seeds/fuel_types.csv",
                "edits": [{"old": last, "new": new}],
                "title": "t",
                "rationale": "r",
            },
        )
        assert r.is_error and why in r.content["error"], r.content


def test_rejected_rows_are_as_sensitive_as_their_table(runs):
    settings = runs["unit_change"]
    run = LocalRunLog(settings.runs_dir, settings.lineage_dir).latest()
    inc = Incident(
        "inc-x-1", "quality_spike", run.run_id, run.started, table="silver.demand_hourly"
    )
    public = _toolbox(settings, inc, run, clearance="public")
    r = public.call("inspect_quarantine", {"table": "silver.demand_hourly"})
    assert r.is_error and "access denied" in r.content["error"]
    internal = _toolbox(settings, inc, run).call(
        "inspect_quarantine", {"table": "silver.demand_hourly", "limit": 50}
    )
    assert internal.content["same_entity"]["value"] == "ERCO"
    assert len(internal.content["samples"]) == 10  # capped
    assert (
        _toolbox(settings, inc, run)
        .call("inspect_quarantine", {"table": "silver.grid_telemetry_minute"})
        .is_error
    )


def test_the_planner_raises_a_bound_when_the_data_is_right(contracts):
    yaml_text = (REPO / "contracts/silver.demand_hourly.yaml").read_text()
    inc = {
        "id": "inc-1",
        "kind": "quality_spike",
        "table": "silver.demand_hourly",
        "rule": "range(demand_mwh:0..250000)",
        "failed_rows": 40,
        "total_rows": 1000,
    }
    history = [
        (
            "get_lineage",
            {"table": "silver.demand_hourly"},
            {"owner": "grid-analytics@gridlake.example", "upstream": []},
            False,
        ),
        (
            "get_contract",
            {"table": "silver.demand_hourly"},
            {"path": "contracts/silver.demand_hourly.yaml", "version": "1.0.0", "yaml": yaml_text},
            False,
        ),
        (
            "inspect_quarantine",
            {"table": "silver.demand_hourly"},
            {
                "column": "demand_mwh",
                "quarantined_rows": 40,
                "rejected_profile": {
                    "rows": 40,
                    "nulls": 0,
                    "min": 251000.0,
                    "median": 262000.0,
                    "max": 281000.0,
                },
                "same_entity": {
                    "key": "ba_code",
                    "value": "PJM",
                    "share_of_rejected_rows": 1.0,
                    "rejected": {"median": 262000.0},
                    "accepted": {"median": 231000.0},
                },
            },
            False,
        ),
    ]
    plan = OfflineResponder()._diagnose(inc, history)
    assert plan["diagnosis"]["category"] == "contract_out_of_date"
    tool, fix = plan["draft"]
    assert tool == "draft_fix"
    assert fix["edits"][0]["new"].endswith(
        "max: 500000, severity: error}"
    )  # 281000 * 1.25 -> 500000
    assert fix["edits"][1] == {"old": "version: 1.0.0\n", "new": "version: 1.1.0\n"}


# -- approval -------------------------------------------------------------------


def _pending_issue(store, tmp_path):
    from gridlake.reliability.drafts import Draft

    return store.save(
        Draft("inc-a-1-d1", "inc-a-1", "issue", "ERCO demand in kWh", "body", ["upstream"])
    )


def test_approving_an_issue_files_it_on_github(tmp_path):
    store = DraftStore(tmp_path / "incidents")
    _pending_issue(store, tmp_path)
    calls = []

    class Session:
        def post(self, url, headers, json, timeout):
            calls.append((url, headers["Authorization"], json))
            return SimpleNamespace(
                raise_for_status=lambda: None,
                json=lambda: {"html_url": "https://github.com/o/r/issues/7"},
            )

    gh = GitHubIssues("o/r", "tok", session=Session())
    draft = approve(store, "inc-a-1-d1", "taojian", tmp_path / "out", tmp_path, github=gh)
    assert draft.outcome == "filed https://github.com/o/r/issues/7"
    assert calls == [
        (
            "https://api.github.com/repos/o/r/issues",
            "Bearer tok",
            {"title": "ERCO demand in kWh", "body": "body", "labels": ["upstream"]},
        )
    ]


def test_without_a_token_an_approved_issue_is_exported_and_rejects_are_recorded(tmp_path):
    store = DraftStore(tmp_path / "incidents")
    _pending_issue(store, tmp_path)
    draft = approve(store, "inc-a-1-d1", "taojian", tmp_path / "out", tmp_path)
    assert "gh issue create" in draft.outcome and (tmp_path / "out/inc-a-1-d1.md").exists()
    with pytest.raises(ValueError):
        reject(store, "inc-a-1-d1", "taojian", "dup")
    assert [d.status for d in store.list()] == ["approved"]


def test_draft_store_on_s3():
    objects = {}

    class S3:
        def put_object(self, Bucket, Key, Body):
            objects[(Bucket, Key)] = Body

        def get_object(self, Bucket, Key):
            return {"Body": SimpleNamespace(read=lambda: objects[(Bucket, Key)])}

        def get_paginator(self, _):
            return SimpleNamespace(
                paginate=lambda Bucket, Prefix: [
                    {
                        "Contents": [
                            {"Key": k} for b, k in objects if b == Bucket and k.startswith(Prefix)
                        ]
                    }
                ]
            )

    from gridlake.reliability.drafts import Draft

    store = DraftStore(None, "s3://bucket/incidents/", client=S3())
    store.save(Draft("inc-b-1-d1", "inc-b-1", "issue", "t", "b"))
    assert ("bucket", "incidents/inc-b-1-d1.json") in objects
    assert [d.id for d in store.list("pending")] == ["inc-b-1-d1"]
    assert store.next_id("inc-b-1") == "inc-b-1-d2"


# -- Step Functions run log --------------------------------------------------------


def test_step_functions_history_becomes_a_run_record():
    start = datetime(2026, 10, 26, 5, 0, tzinfo=UTC)
    cause = json.dumps(
        {
            "errorType": "QualityGateError",
            "errorMessage": "quality gate failed for gold.ba_daily_energy: unique(date,ba_code) (2 rows)",
        }
    )
    events = [
        {"id": 1, "type": "TaskStateEntered", "stateEnteredEventDetails": {"name": "Silver"}},
        {"id": 2, "type": "TaskStateExited", "stateExitedEventDetails": {"name": "Silver"}},
        {"id": 3, "type": "TaskStateEntered", "stateEnteredEventDetails": {"name": "Gold"}},
        {
            "id": 4,
            "type": "TaskFailed",
            "taskFailedEventDetails": {"error": "QualityGateError", "cause": cause},
        },
        {
            "id": 5,
            "type": "TaskStateEntered",
            "stateEnteredEventDetails": {"name": "NotifyFailure"},
        },
    ]

    class Sfn:
        def describe_execution(self, executionArn):
            return {
                "executionArn": executionArn,
                "status": "FAILED",
                "startDate": start,
                "stopDate": start + timedelta(minutes=2),
            }

        def get_execution_history(self, **kw):
            return {"events": events}

    log = StepFunctionsRunLog("arn:sm", "us-east-2", client=Sfn())
    run = log.get("arn:aws:states:us-east-2:1:execution:gridlake-dev-pipeline:abc-123")
    assert [(s.name, s.status) for s in run.steps] == [("Silver", "COMPLETE"), ("Gold", "FAIL")]
    assert run.steps[1].error.startswith(
        "QualityGateError: quality gate failed for gold.ba_daily_energy"
    )
    assert run.quality_run_ids is None and run.ended == (start + timedelta(minutes=2)).isoformat()
    ts = pa.timestamp("us", tz="UTC")
    quality = pa.table(
        {
            "run_id": ["a", "b"],
            "evaluated_ts": pa.array(
                [start + timedelta(minutes=1), start - timedelta(hours=1)], ts
            ),
        }
    )
    assert quality_for_run(quality, run).column("run_id").to_pylist() == ["a"]
    [inc] = detect_incidents(run, pa.table({}))
    assert inc.id == "inc-abc123-1" and inc.table == "gold.ba_daily_energy"


def test_cli_triage_and_incidents(tmp_path, monkeypatch, capsys):
    from gridlake.cli import main

    monkeypatch.setenv("GRIDLAKE_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    args = ["run", "--end", "2026-10-06", "--days", "5", "--telemetry-hours", "1"]
    assert main([*args, "--inject-fault", "feed_outage"]) == 1
    assert "FAILED" in capsys.readouterr().out
    assert main(["triage", "--trace"]) == 0
    out = capsys.readouterr().out
    assert "upstream_outage" in out and "> record_diagnosis(" in out
    assert main(["incidents", "list"]) == 0
    draft = capsys.readouterr().out.split()[0]
    assert main(["incidents", "show", draft]) == 0
    assert "[pending]" in capsys.readouterr().out
    assert main(["incidents", "reject", draft, "--by", "taojian", "--reason", "EIA is back"]) == 0
    assert "rejected by taojian" in capsys.readouterr().out
    assert main(["incidents", "list"]) == 0
    assert capsys.readouterr().out == ""


def test_claude_drives_the_same_loop(runs):
    """BedrockClaude with a stubbed client: the model's tool calls run under the bound clearance."""
    from gridlake.agent.llm import BedrockClaude
    from gridlake.reliability.agent import ReliabilityAgent

    settings = runs["feed_outage"]
    run = LocalRunLog(settings.runs_dir, settings.lineage_dir).latest()
    [inc] = detect_incidents(run, pa.table({}))
    script = [
        [{"type": "tool_use", "id": "a", "name": "get_run_log", "input": {}}],
        [
            {
                "type": "tool_use",
                "id": "b",
                "name": "record_diagnosis",
                "input": {
                    "category": "upstream_outage",
                    "cause": "EIA returned 503.",
                    "evidence": ["HTTP 503"],
                    "confidence": "high",
                },
            }
        ],
        [
            {
                "type": "tool_use",
                "id": "c",
                "name": "draft_issue",
                "input": {"title": "EIA outage", "body": "Re-run later."},
            }
        ],
        [{"type": "text", "text": "Upstream outage; drafted an issue."}],
    ]
    seen = []

    class Messages:
        def create(self, **kw):
            seen.append(kw)
            content = [SimpleNamespace(**b) for b in script[len(seen) - 1]]
            stop = "tool_use" if content[0].type == "tool_use" else "end_turn"
            return SimpleNamespace(stop_reason=stop, content=content)

    llm = BedrockClaude(
        "anthropic.claude-opus-5-5", "us-east-2", client=SimpleNamespace(messages=Messages())
    )
    t = ReliabilityAgent(llm).triage(inc, _toolbox(settings, inc, run))
    assert t.summary == "Upstream outage; drafted an issue."
    assert [d.title for d in t.drafts] == ["EIA outage"] and t.diagnosis[
        "category"
    ] == "upstream_outage"
    assert {s["name"] for s in seen[0]["tools"]} >= {"get_run_log", "draft_fix", "run_sql"}
    assert "lookup_entity" not in {s["name"] for s in seen[0]["tools"]}
    assert seen[0]["system"].startswith("You are gridlake's data reliability engineer")
