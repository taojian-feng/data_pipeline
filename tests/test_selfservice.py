"""The self-service data product agent: declared products, governance, drafts and the PR."""

import shutil
import subprocess
import sys
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from conftest import REPO, START
from gridlake import lineage as ol
from gridlake.agent.tools import LocalSql
from gridlake.catalog import Lakehouse, get_catalog
from gridlake.config import Settings
from gridlake.contracts import _parse, load_contracts
from gridlake.pipeline import Pipeline
from gridlake.reliability.drafts import DraftStore
from gridlake.selfservice import ProductToolbox, Request, design_product, drafting
from gridlake.selfservice.planner import plan
from gridlake.selfservice.publish import GitHubPulls, approve, branch_name, open_pull_request
from gridlake.sources.eia import SyntheticEIASource
from gridlake.transform import declared

END = START + timedelta(days=10)
EXAMPLE = "gold.ba_weekly_energy"  # the product the agent drafted for this repo


def _repo(base: Path) -> Path:
    """A copy of the repo's contracts, seeds and transforms that approvals may change."""
    repo = base / "repo"
    for name in ("contracts", "seeds", "transforms"):
        shutil.copytree(REPO / name, repo / name)
    return repo


def _settings(home: Path, repo: Path) -> Settings:
    return Settings(
        home=home,
        catalog_type="local",
        eia_api_key=None,
        openlineage_url=None,
        agent_llm="offline",
        agent_sql="local",
        kb_s3_uri=None,
        graph_s3_uri=None,
        contracts_dir=repo / "contracts",
        seeds_dir=repo / "seeds",
        transforms_dir=repo / "transforms",
    )


@pytest.fixture(scope="module")
def lake(tmp_path_factory, bas):
    base = tmp_path_factory.mktemp("selfservice")
    repo = _repo(base)
    settings = _settings(base / "home", repo)
    Pipeline(settings, source=SyntheticEIASource(bas)).run(START, END, telemetry_hours=1)
    return settings


@pytest.fixture
def fresh(lake, tmp_path):
    """The module's lakehouse, with its own repo copy and draft store per test."""
    repo = _repo(tmp_path)
    home = tmp_path / "home"
    shutil.copytree(lake.home, home)
    return _settings(home, repo)


def _propose(settings, text, clearance="internal", **kw):
    request = Request(text, kw.pop("owner", "sustainability@gridlake.example"), **kw)
    return design_product(
        settings, request, clearance=clearance, repo_root=settings.contracts_dir.parent
    )


# -- declared products in the pipeline ------------------------------------------


def test_the_example_product_is_built_by_the_pipeline(lake):
    lakehouse = Lakehouse(get_catalog(lake), load_contracts(lake.contracts_dir))
    weekly = lakehouse.read(EXAMPLE)
    daily = lakehouse.read("gold.ba_daily_energy")
    assert weekly.num_rows and weekly.num_rows < daily.num_rows
    assert sum(weekly.column("total_demand_mwh").to_pylist()) == pytest.approx(
        sum(daily.column("total_demand_mwh").to_pylist())
    )
    starts = set(weekly.column("week_start").to_pylist())
    assert all(d.weekday() == 0 for d in starts)  # Mondays
    edges = {(s, d) for s, _, d in ol.read_edges(lake.lineage_dir)}
    assert ("gold.ba_daily_energy", EXAMPLE) in edges


def test_a_product_rebuilds_only_the_periods_whose_inputs_changed(lake, tmp_path, bas):
    settings = replace(lake, home=tmp_path / "home")
    shutil.copytree(lake.home, settings.home)
    pipe = Pipeline(settings, source=SyntheticEIASource(bas))
    before = pipe.lake.table(EXAMPLE).current_snapshot().snapshot_id
    pipe.build_products({"gold.ba_daily_energy": [date(2026, 10, 7)]})
    [step] = pipe.steps
    assert step.name == "product_ba_weekly_energy" and step.status == "COMPLETE"
    assert step.metrics["rows"] == 8  # one week (2026-10-05) for each of the eight BAs
    assert pipe.lake.table(EXAMPLE).current_snapshot().snapshot_id != before
    pipe.build_products({"silver.dim_fuel_type": [date(2026, 10, 7)]})
    assert len(pipe.steps) == 1  # inputs unchanged: nothing to rebuild


def test_a_product_that_breaks_governance_fails_its_step(lake, tmp_path, bas):
    repo = _repo(tmp_path)
    path = repo / "contracts" / f"{EXAMPLE}.yaml"
    raw = yaml.safe_load(path.read_text())
    raw["transform"]["inputs"] = ["gold.ba_daily_energy", "gold.grid_reliability_daily"]
    path.write_text(yaml.safe_dump(raw))
    settings = _settings(tmp_path / "home", repo)
    shutil.copytree(lake.home, settings.home)
    pipe = Pipeline(settings, source=SyntheticEIASource(bas))
    with pytest.raises(declared.ProductError, match="below its input"):
        pipe.build_products({"gold.ba_daily_energy": [date(2026, 10, 7)]})
    assert pipe.steps[-1].status == "FAIL"


def test_governance_rules(contracts):
    good = contracts[EXAMPLE]
    sql = declared.load_sql(good, REPO / "transforms")
    assert declared.check_product(good, contracts, sql) == []

    def problems(sql=sql, **changes):
        return " ".join(declared.check_product(replace(good, **changes), contracts, sql))

    assert "below its input" in problems(
        sql="SELECT * FROM gold.grid_reliability_daily",
        transform={**good.transform, "inputs": ["gold.grid_reliability_daily"]},
    )
    assert "silver or gold" in problems(
        transform={**good.transform, "inputs": ["bronze.eia_region_data"]}
    )
    assert "does not declare" in problems(
        sql=sql.replace("gold.ba_daily_energy", "gold.fuel_mix_daily")
    )
    assert "single read-only SELECT" in problems(sql="DELETE FROM gold.ba_daily_energy")
    assert "unique rule" in problems(quality=good.quality[1:])
    assert "team email" in problems(owner="someone")
    assert "consumer" in problems(consumers=())
    assert "need a description" in problems(
        columns=(replace(good.columns[0], description=""), *good.columns[1:])
    )
    assert "grain must be" in problems(transform={**good.transform, "grain": "hour"})
    assert "partition by one date" in problems(partition_by=({"column": "ba_code"},))
    # A table named only in a comment is not an input.
    assert declared.check_product(good, contracts, "-- reads gold.fuel_mix_daily\n" + sql) == []


def test_period_start_and_ordering(contracts):
    assert declared.period_start(date(2026, 10, 8), "week") == date(2026, 10, 5)
    assert declared.period_start(date(2026, 10, 8), "month") == date(2026, 10, 1)
    assert declared.period_start(date(2026, 10, 8), "day") == date(2026, 10, 8)
    base = contracts[EXAMPLE]
    child = replace(
        base, name="gold.ba_weekly_child", transform={**base.transform, "inputs": [EXAMPLE]}
    )
    order = declared.declared_products({**contracts, child.name: child})
    assert [c.name for c in order][-2:] == [EXAMPLE, child.name]
    loop = replace(base, transform={**base.transform, "inputs": [child.name]})
    with pytest.raises(declared.ProductError, match="cycle"):
        declared.declared_products({EXAMPLE: loop, child.name: child})


def test_contract_rendering_round_trips():
    tricky = 'Totals: demand, "peak" & net-generation # per BA-day; yes'
    spec = {
        "name": "gold.x_daily",
        "version": "1.0.0",
        "layer": "gold",
        "domain": "grid-operations",
        "owner": "a@b.example",
        "classification": "internal",
        "description": tricky,
        "primary_key": ["date"],
        "partition_by": [{"column": "date", "transform": "identity"}],
        "consumers": ["bi-dashboards"],
        "transform": {"sql": "transforms/gold.x_daily.sql", "inputs": ["gold.a"], "grain": "day"},
        "columns": [{"name": "date", "type": "date", "nullable": False, "description": tricky}],
        "quality": [{"rule": "expression", "sql": "a <= b", "severity": "error"}],
    }
    loaded = yaml.safe_load(drafting.render_contract(spec))
    assert " ".join(loaded.pop("description").split()) == tricky
    assert loaded == {k: v for k, v in spec.items() if k != "description"}
    assert _parse(yaml.safe_load(drafting.render_contract(spec)), Path("gold.x_daily.yaml"))


# -- the agent --------------------------------------------------------------------


def test_offline_designer_drafts_contract_sql_and_tests(fresh):
    d = _propose(fresh, "Monthly demand and emissions by region for the sustainability report")
    assert [s.tool for s in d.steps] == [
        "list_tables",
        "get_contract",
        "preview_transform",
        "draft_data_product",
    ]
    [draft] = d.drafts
    name = "gold.region_monthly_energy"
    assert (
        draft.kind == "product"
        and draft.status == "pending"
        and draft.path == f"contracts/{name}.yaml"
    )
    files = [
        line.split(" b/")[1] for line in draft.patch.splitlines() if line.startswith("diff --git")
    ]
    assert files == [
        f"contracts/{name}.yaml",
        f"transforms/{name}.sql",
        f"tests/products/test_{name.replace('.', '_')}.py",
    ]
    assert not (fresh.contracts_dir / f"{name}.yaml").exists()  # nothing changes until approval
    assert "public" in d.summary and draft.id in d.summary
    assert (
        "## Governance" in draft.body and "Check passed: total_demand_mwh reconciles" in draft.body
    )
    assert "sustainability-reporting" in draft.body
    assert "peak" not in draft.patch  # not asked for
    assert "total_emissions_t" in draft.patch and "region_name" in draft.patch


def test_an_approved_draft_builds_in_the_pipeline_and_its_tests_pass(fresh, bas):
    d = _propose(fresh, "Daily renewable share per interconnection for the ESG dashboard")
    [draft] = d.drafts
    repo = fresh.contracts_dir.parent
    done = approve(
        DraftStore(fresh.proposals_dir), draft.id, "taojian", fresh.home / "ok", repo, apply=True
    )
    assert done.status == "approved"
    name = "gold.interconnection_daily_renewable_share"
    contracts = load_contracts.__wrapped__(repo / "contracts")  # uncached: the files just changed
    assert contracts[name].classification == "public"
    assert contracts[name].transform["inputs"] == [
        "gold.fuel_mix_daily",
        "silver.dim_balancing_authority",
        "silver.dim_fuel_type",
    ]
    # The drafted tests run as they will in CI on the pull request.
    test = repo / "tests" / "products" / f"test_{name.replace('.', '_')}.py"
    run = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", str(test)],
        cwd=repo,
        capture_output=True,
        text=True,
    )
    assert run.returncode == 0, run.stdout + run.stderr
    assert "6 passed" in run.stdout


def test_classification_never_drops_below_the_inputs(fresh):
    d = _propose(
        fresh,
        "Weekly frequency excursions per balancing authority for a public dashboard",
        clearance="confidential",
    )
    [draft] = d.drafts
    assert "classification: confidential" in draft.patch
    assert "You asked for public, but the inputs make it confidential" in d.summary


def test_the_tool_refuses_a_declassified_product(fresh):
    contracts = load_contracts(fresh.contracts_dir)
    lakehouse = Lakehouse(get_catalog(fresh), contracts)
    box = ProductToolbox(
        contracts,
        "confidential",
        LocalSql(lakehouse.read),
        lakehouse.read,
        "weekly excursions",
        "ops@gridlake.example",
        DraftStore(fresh.proposals_dir),
        fresh.contracts_dir.parent,
        fresh.contracts_dir,
        fresh.transforms_dir,
    )
    p = plan("weekly frequency excursions per balancing authority")
    args = {
        "name": p.name,
        "description": "Weekly excursions.",
        "classification": "public",
        "consumers": ["bi-dashboards"],
        "grain": "week",
        "primary_key": [p.period, "ba_code"],
        "columns": p.columns(),
        "sql": p.sql(),
        "checks": [{"name": k, "sql": v} for k, v in p.checks(p.name).items()],
        "quality": p.rules,
        "rationale": "test",
    }
    r = box.call("draft_data_product", args)
    assert r.is_error and "below its input gold.grid_reliability_daily" in r.content["error"]
    r = box.call("draft_data_product", {**args, "checks": []})
    assert r.is_error and "at least one check" in r.content["error"]
    bad = [{"name": "always fails", "sql": f"SELECT * FROM {p.name}"}]
    r = box.call("draft_data_product", {**args, "classification": "confidential", "checks": bad})
    assert r.is_error and "checks fail" in r.content["error"]
    r = box.call(
        "draft_data_product",
        {
            **args,
            "classification": "confidential",
            "sql": p.sql().replace(
                "sum(s.excursion_minutes)", "sum(s.excursion_minutes) + 1000000000"
            ),
        },
    )
    assert r.is_error and "fails the drafted rules" in r.content["error"]
    r = box.call("preview_transform", {"sql": "SELECT * FROM bronze.eia_region_data"})
    assert r.is_error and "conformed silver or gold" in r.content["error"]
    assert box.created == []
    r = box.call("draft_data_product", {**args, "classification": "confidential"})
    assert not r.is_error and r.content["draft_id"] == "dp-ba_weekly_frequency_excursions-d1"


def test_a_caller_cannot_build_on_tables_above_their_clearance(fresh):
    d = _propose(fresh, "Weekly frequency excursions per balancing authority", clearance="internal")
    assert d.drafts == []
    assert "not a table you are cleared to read" in d.summary


def test_existing_products_and_unsupported_requests_are_not_drafted(fresh):
    d = _propose(fresh, "Weekly demand per balancing authority for the BI dashboard")
    assert d.drafts == [] and "already exists" in d.summary
    d = _propose(fresh, "A list of substations with their GPS coordinates")
    assert d.drafts == [] and "outside that" in d.summary


def test_claude_drives_the_same_tools(fresh):
    """BedrockClaude with a stubbed client: the model's calls run under the bound clearance."""
    from gridlake.agent.llm import BedrockClaude
    from gridlake.selfservice import ProductDesigner

    script = [
        [
            {
                "type": "tool_use",
                "id": "a",
                "name": "preview_transform",
                "input": {"sql": "SELECT * FROM gold.grid_reliability_daily"},
            }
        ],
        [{"type": "text", "text": "You are not cleared for that table."}],
    ]
    seen = []

    class Messages:
        def create(self, **kw):
            seen.append(kw)
            content = [SimpleNamespace(**b) for b in script[len(seen) - 1]]
            stop = "tool_use" if content[0].type == "tool_use" else "end_turn"
            return SimpleNamespace(stop_reason=stop, content=content)

    contracts = load_contracts(fresh.contracts_dir)
    lakehouse = Lakehouse(get_catalog(fresh), contracts)
    box = ProductToolbox(
        contracts,
        "internal",
        LocalSql(lakehouse.read),
        lakehouse.read,
        "x",
        "a@b.example",
        DraftStore(fresh.proposals_dir),
        fresh.contracts_dir.parent,
        fresh.contracts_dir,
        fresh.transforms_dir,
    )
    llm = BedrockClaude(
        "anthropic.claude-opus-5-5", "us-east-2", client=SimpleNamespace(messages=Messages())
    )
    d = ProductDesigner(llm).design(Request("x", "a@b.example"), box)
    assert d.steps[0].ok is False and "access denied" in d.steps[0].error
    assert {s["name"] for s in seen[0]["tools"]} == {
        "list_tables",
        "run_sql",
        "describe_table",
        "get_contract",
        "preview_transform",
        "draft_data_product",
    }
    assert seen[0]["system"].startswith("You are gridlake's data product engineer")


# -- approval opens the pull request ------------------------------------------------


def _git(cwd, *args):
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout


def test_approval_commits_on_a_branch_and_opens_the_pull_request(fresh, tmp_path):
    repo = fresh.contracts_dir.parent
    remote = tmp_path / "remote.git"
    _git(tmp_path, "init", "-q", "--bare", str(remote))
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@example.com", "add", ".")
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "base")
    _git(repo, "remote", "add", "origin", str(remote))
    _git(repo, "push", "-q", "origin", "main")

    d = _propose(fresh, "Monthly peak demand per balancing authority for the BI dashboard")
    [draft] = d.drafts
    posted = []

    class Session:
        def post(self, url, headers, json, timeout):
            posted.append((url, json))
            return SimpleNamespace(
                raise_for_status=lambda: None,
                json=lambda: {"html_url": "https://github.com/o/r/pull/9"},
            )

    store = DraftStore(fresh.proposals_dir)
    done = open_pull_request(
        store, draft.id, "taojian", repo, github=GitHubPulls("o/r", "t", Session())
    )
    assert done.status == "approved" and done.outcome == "opened https://github.com/o/r/pull/9"
    branch = branch_name(draft)
    assert branch == "data-product/ba-monthly-energy"
    [(url, body)] = posted
    assert url.endswith("/repos/o/r/pulls")
    assert body["head"] == branch and body["base"] == "main" and body["title"] == draft.title
    files = _git(remote, "ls-tree", "-r", "--name-only", branch).split()
    assert "contracts/gold.ba_monthly_energy.yaml" in files
    assert "tests/products/test_gold_ba_monthly_energy.py" in files
    assert "Approved-by: taojian" in _git(remote, "log", "-1", "--format=%B", branch)
    assert _git(repo, "status", "--porcelain") == ""  # the person's checkout is untouched
    assert "worktree" not in _git(repo, "worktree", "list").split("\n", 1)[-1]
    with pytest.raises(ValueError, match="already approved"):
        open_pull_request(store, draft.id, "taojian", repo)


def test_cli_propose_and_products(fresh, monkeypatch, capsys):
    from gridlake.cli import main

    monkeypatch.setenv("GRIDLAKE_HOME", str(fresh.home))
    monkeypatch.setenv("GRIDLAKE_AGENT_LLM", "offline")
    repo = fresh.contracts_dir.parent
    monkeypatch.chdir(repo)
    # Settings() reads the packaged contracts; point the CLI at the copy instead.
    import gridlake.cli as cli

    monkeypatch.setattr(cli, "Settings", lambda: fresh)
    req = "Weekly emissions by interconnection for the sustainability report"
    assert main(["propose", req, "--owner", "esg@gridlake.example", "--trace"]) == 0
    out = capsys.readouterr().out
    assert "> draft_data_product(" in out and "gold.interconnection_weekly_energy" in out
    assert main(["products", "list"]) == 0
    draft = capsys.readouterr().out.split()[0]
    assert main(["products", "show", draft, "--body"]) == 0
    assert capsys.readouterr().out.startswith("## Request")
    assert main(["products", "reject", draft, "--by", "taojian", "--reason", "use monthly"]) == 0
    assert "rejected by taojian" in capsys.readouterr().out
    assert main(["propose", "GPS coordinates of substations", "--owner", "x@gridlake.example"]) == 1
