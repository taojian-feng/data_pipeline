import json
from datetime import date
from types import SimpleNamespace

import pyarrow as pa
import pytest

from conftest import REPO
from gridlake.agent import bedrock_action_group as ag
from gridlake.agent.core import GridAnalyst
from gridlake.agent.llm import BedrockClaude, OfflinePlanner
from gridlake.agent.tools import (
    AthenaSql,
    BedrockKbRetriever,
    LocalRetriever,
    LocalSql,
    Toolbox,
    bedrock_filter,
    referenced_tables,
)
from gridlake.knowledge.graph import Ontology, build_graph
from gridlake.knowledge.retrieval import Document, VectorIndex
from gridlake.transform import silver


def _daily(ba, d, renewable, carbon):
    return {
        "date": date.fromisoformat(d),
        "ba_code": ba,
        "renewable_share_pct": renewable,
        "carbon_intensity_kg_per_mwh": carbon,
        "total_demand_mwh": 1000.0,
    }


TABLES = {
    "gold.ba_daily_energy": pa.Table.from_pylist(
        [
            _daily("CISO", "2026-10-01", 40.0, 200.0),
            _daily("CISO", "2026-10-02", 30.0, 240.0),
            _daily("ERCO", "2026-10-01", 20.0, 380.0),
            _daily("ERCO", "2026-10-02", 22.0, 370.0),
        ]
    ),
    "gold.grid_reliability_daily": pa.Table.from_pylist(
        [{"date": date(2026, 10, 1), "ba_code": "ERCO", "excursion_minutes": 12}]
    ),
}


@pytest.fixture(scope="module")
def graph(contracts):
    return build_graph(
        Ontology.load(),
        silver.dim_balancing_authority(REPO / "seeds").to_pylist(),
        silver.dim_fuel_type(REPO / "seeds").to_pylist(),
        contracts.values(),
        [("silver.demand_hourly", "gold_ba_daily_energy", "gold.ba_daily_energy")],
        substations=[("SUB-ERCO-001", "ERCO")],
    )


def _index():
    return VectorIndex().build(
        [
            Document(
                "ba_daily_energy/CISO/2026-10-01",
                "On 2026-10-01 CISO renewables supplied 40% of generation",
                {
                    "classification": "public",
                    "doc_type": "fact",
                    "ba_code": "CISO",
                    "source_table": "gold.ba_daily_energy",
                },
            ),
            Document(
                "grid_reliability_daily/ERCO/2026-10-01",
                "ERCO had 12 frequency excursion minutes",
                {
                    "classification": "confidential",
                    "doc_type": "fact",
                    "ba_code": "ERCO",
                    "source_table": "gold.grid_reliability_daily",
                },
            ),
        ]
    )


def toolbox(contracts, graph, clearance="public"):
    return Toolbox(
        contracts,
        clearance,
        LocalSql(TABLES.__getitem__),
        LocalRetriever(_index),
        graph=lambda: graph,
    )


# -- SQL guard ------------------------------------------------------------------


def test_referenced_tables_sees_through_ctes_joins_and_subqueries():
    sql = """WITH c AS (SELECT * FROM gold.ba_daily_energy)
             SELECT * FROM c JOIN silver.dim_balancing_authority d USING (ba_code)
             WHERE ba_code IN (SELECT ba_code FROM gold.grid_reliability_daily)"""
    assert referenced_tables(sql) == {
        "gold.ba_daily_energy",
        "silver.dim_balancing_authority",
        "gold.grid_reliability_daily",
    }


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM gold.ba_daily_energy",
        "SELECT 1; SELECT 2",
        "SELECT * FROM read_csv('/etc/passwd')",
        "SELECT * FROM gold.ba_daily_energy, read_parquet('s3://bucket/x.parquet')",
    ],
)
def test_only_single_selects_over_governed_tables(sql):
    with pytest.raises(ValueError):
        referenced_tables(sql)


def test_non_strict_parse_falls_back_to_qualified_names():
    # Iceberg time travel in Athena's dialect, which DuckDB does not parse
    trino = (
        "SELECT * FROM gold.ba_daily_energy FOR TIMESTAMP AS OF TIMESTAMP '2026-10-01 00:00:00 UTC'"
    )
    assert "gold.ba_daily_energy" in referenced_tables(trino, strict=False)
    with pytest.raises(ValueError):
        referenced_tables("DROP TABLE gold.ba_daily_energy", strict=False)


def test_run_sql_enforces_clearance_before_querying(contracts, graph):
    sql = "SELECT ba_code, sum(excursion_minutes) AS m FROM gold.grid_reliability_daily GROUP BY 1"
    denied = toolbox(contracts, graph, "internal").call("run_sql", {"sql": sql})
    assert denied.is_error and "access denied" in denied.content["error"]
    allowed = toolbox(contracts, graph, "confidential").call("run_sql", {"sql": sql})
    assert allowed.content["rows"] == [["ERCO", 12]]
    assert allowed.tables == ["gold.grid_reliability_daily"]
    unknown = toolbox(contracts, graph).call("run_sql", {"sql": "SELECT * FROM gold.secrets"})
    assert unknown.is_error and "unknown table" in unknown.content["error"]


def test_list_tables_shows_only_what_the_caller_can_query(contracts, graph):
    public = {
        t["table"] for t in toolbox(contracts, graph).call("list_tables", {}).content["tables"]
    }
    assert "gold.ba_daily_energy" in public
    assert not public & {
        "gold.grid_reliability_daily",
        "silver.demand_hourly",
        "quality.quarantine",
    }


def test_catalog_metadata_is_never_more_open_than_internal(contracts, graph):
    public = toolbox(contracts, graph).call("describe_table", {"table": "gold.ba_daily_energy"})
    assert public.is_error and "needs internal clearance" in public.content["error"]
    internal = toolbox(contracts, graph, "internal").call(
        "describe_table", {"table": "gold.ba_daily_energy"}
    )
    assert internal.content["owner"] == "grid-analytics@gridlake.example"
    assert internal.content["derived_from"] == ["silver.demand_hourly"]


def test_model_cannot_pass_its_own_clearance(contracts, graph):
    result = toolbox(contracts, graph).call(
        "run_sql", {"sql": "SELECT 1", "clearance": "confidential"}
    )
    assert result.is_error and "invalid request" in result.content["error"]


# -- graph and documents --------------------------------------------------------


def test_lookup_entity_filters_graph_facts_by_clearance(contracts, graph):
    text = "ERCOT and gold.grid_reliability_daily and SUB-ERCO-001"
    public = toolbox(contracts, graph).call("lookup_entity", {"text": text}).content["entities"]
    assert [e["code"] for e in public] == ["ERCO"]
    assert not any("SUB-ERCO-001" in f or "grid_reliability" in f for f in public[0]["facts"])
    top = toolbox(contracts, graph, "confidential").call("lookup_entity", {"text": text})
    by_label = {e["label"]: e for e in top.content["entities"]}
    assert "gold.grid_reliability_daily" in by_label
    erco = by_label["Electric Reliability Council of Texas, Inc."]
    assert any("SUB-ERCO-001" in f for f in erco["facts"])


def test_search_documents_cites_sources_within_clearance(contracts, graph):
    result = toolbox(contracts, graph).call("search_documents", {"query": "frequency excursion"})
    assert result.content["passages"] == []
    result = toolbox(contracts, graph, "confidential").call(
        "search_documents", {"query": "frequency excursion", "ba_code": "ERCO"}
    )
    assert result.documents == ["grid_reliability_daily/ERCO/2026-10-01"]
    assert result.tables == ["gold.grid_reliability_daily"]


class FakeKb:
    def __init__(self):
        self.calls = []

    def retrieve(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "retrievalResults": [
                {
                    "content": {"text": "ok"},
                    "score": 0.9,
                    "metadata": {
                        "classification": "public",
                        "doc_id": "a",
                        "source_table": "gold.x",
                    },
                },
                {
                    "content": {"text": "mis-tagged"},
                    "score": 0.8,
                    "metadata": {"classification": "confidential", "doc_id": "b"},
                },
            ]
        }


def test_bedrock_retrieve_filters_by_classification_server_and_client_side():
    assert bedrock_filter("internal", {}) == {
        "in": {"key": "classification", "value": ["internal", "public"]}
    }
    kb = FakeKb()
    hits = BedrockKbRetriever("KB123", "us-east-2", client=kb).search(
        "renewables", "public", {"ba_code": "CISO"}, 3
    )
    assert [h["doc_id"] for h in hits] == ["a"]  # the mis-tagged hit never reaches the model
    config = kb.calls[0]["retrievalConfiguration"]["vectorSearchConfiguration"]
    assert config["numberOfResults"] == 3
    assert config["filter"] == {
        "andAll": [
            {"in": {"key": "classification", "value": ["public"]}},
            {"equals": {"key": "ba_code", "value": "CISO"}},
        ]
    }


class FakeAthena:
    def __init__(self, states):
        self.states, self.started = list(states), []

    def start_query_execution(self, **kwargs):
        self.started.append(kwargs)
        return {"QueryExecutionId": "q1"}

    def get_query_execution(self, QueryExecutionId):
        state = self.states.pop(0)
        return {"QueryExecution": {"Status": {"State": state, "StateChangeReason": "boom"}}}

    def get_paginator(self, name):
        rows = [["ba_code", "n"], ["CISO", "2"], ["ERCO", "2"]]
        page = {
            "ResultSet": {
                "ResultSetMetadata": {"ColumnInfo": [{"Name": "ba_code"}, {"Name": "n"}]},
                "Rows": [{"Data": [{"VarCharValue": v} for v in r]} for r in rows],
            }
        }
        return SimpleNamespace(paginate=lambda **_: [page])


def test_athena_engine_polls_and_reads_results():
    athena = FakeAthena(["QUEUED", "RUNNING", "SUCCEEDED"])
    engine = AthenaSql("gridlake-analyst", "us-east-2", client=athena, poll_s=0)
    cols, rows = engine.run(
        "SELECT ba_code, count(*) n FROM gold.ba_daily_energy GROUP BY 1", set(), 50
    )
    assert cols == ["ba_code", "n"] and rows == [["CISO", "2"], ["ERCO", "2"]]
    assert athena.started[0]["WorkGroup"] == "gridlake-analyst"
    with pytest.raises(RuntimeError, match="boom"):
        AthenaSql("wg", "us-east-2", client=FakeAthena(["FAILED"]), poll_s=0).run(
            "SELECT 1", set(), 5
        )


# -- the agent loop -------------------------------------------------------------


def test_offline_agent_chooses_tools_and_cites_tables(contracts, graph):
    analyst = GridAnalyst(OfflinePlanner(), toolbox(contracts, graph))
    answer = analyst.ask("What was California ISO's renewable share on 2026-10-02?")
    assert [s.tool for s in answer.steps] == ["lookup_entity", "run_sql"]
    assert "ba_code IN ('CISO')" in answer.steps[1].input["sql"]
    assert answer.text == "CISO renewable share on 2026-10-02: 30.0%."
    assert answer.tables == ["gold.ba_daily_energy"]

    ranking = analyst.ask("Which balancing authority has the lowest carbon intensity?")
    assert ranking.text.startswith("CISO ranks first by average carbon intensity (220.0 kg CO2/MWh")


def test_offline_agent_reports_access_denied(contracts, graph):
    answer = GridAnalyst(OfflinePlanner(), toolbox(contracts, graph)).ask(
        "Which balancing authority had the most frequency excursion minutes?"
    )
    assert not answer.steps[-1].ok
    assert "classified confidential; caller clearance is public" in answer.text
    assert answer.tables == []


def _block(**kw):
    return SimpleNamespace(**kw)


class FakeAnthropic:
    """Records requests and replays canned Messages API responses."""

    def __init__(self, responses):
        self.responses, self.requests = list(responses), []
        self.messages = SimpleNamespace(create=self._create)

    def _create(self, **kwargs):
        self.requests.append(json.loads(json.dumps(kwargs, default=lambda o: o.__dict__)))
        return self.responses.pop(0)


def test_bedrock_claude_drives_the_same_loop(contracts, graph):
    fake = FakeAnthropic(
        [
            SimpleNamespace(
                stop_reason="tool_use",
                content=[
                    _block(type="text", text="Checking both."),
                    _block(type="tool_use", id="t1", name="lookup_entity", input={"text": "ERCOT"}),
                    _block(
                        type="tool_use",
                        id="t2",
                        name="run_sql",
                        input={
                            "sql": "SELECT avg(renewable_share_pct) AS r FROM gold.ba_daily_energy WHERE ba_code = 'ERCO'"
                        },
                    ),
                ],
            ),
            SimpleNamespace(
                stop_reason="end_turn",
                content=[_block(type="text", text="ERCOT averaged 21% (gold.ba_daily_energy).")],
            ),
        ]
    )
    llm = BedrockClaude("anthropic.claude-opus-5-5", "us-east-2", client=fake)
    answer = GridAnalyst(llm, toolbox(contracts, graph)).ask("ERCOT renewable share?")
    assert answer.text.startswith("ERCOT averaged 21%")
    assert answer.tables == ["gold.ba_daily_energy"] and answer.model.startswith("bedrock:")
    first, second = fake.requests
    assert first["model"] == "anthropic.claude-opus-5-5"
    assert {t["name"] for t in first["tools"]} >= {"run_sql", "lookup_entity", "search_documents"}
    results = second["messages"][-1]
    assert results["role"] == "user" and [r["tool_use_id"] for r in results["content"]] == [
        "t1",
        "t2",
    ]
    assert json.loads(results["content"][1]["content"])["rows"] == [[21.0]]


def test_refusal_and_step_limit_end_the_loop(contracts, graph):
    refusal = FakeAnthropic([SimpleNamespace(stop_reason="refusal", content=[])])
    answer = GridAnalyst(BedrockClaude("m", "r", client=refusal), toolbox(contracts, graph)).ask(
        "x"
    )
    assert answer.stop_reason == "refusal" and "declined" in answer.text
    loop = [
        SimpleNamespace(
            stop_reason="tool_use",
            content=[_block(type="tool_use", id=f"t{i}", name="list_tables", input={})],
        )
        for i in range(3)
    ]
    capped = GridAnalyst(
        BedrockClaude("m", "r", client=FakeAnthropic(loop)), toolbox(contracts, graph), max_steps=3
    ).ask("x")
    assert capped.stop_reason == "max_steps"


# -- Bedrock Agents action group ------------------------------------------------


def test_action_group_takes_clearance_from_the_session(contracts, graph, monkeypatch):
    seen = []

    def fake_toolbox(clearance):
        seen.append(clearance)
        return toolbox(contracts, graph, clearance)

    monkeypatch.setattr(ag, "_toolbox", fake_toolbox)
    event = {
        "actionGroup": "gridlake-tools",
        "function": "run_sql",
        "parameters": [
            {
                "name": "sql",
                "type": "string",
                "value": "SELECT count(*) AS n FROM gold.grid_reliability_daily",
            }
        ],
        "sessionAttributes": {"clearance": "root"},
    }
    out = ag.handler(event, None)
    body = json.loads(out["response"]["functionResponse"]["responseBody"]["TEXT"]["body"])
    assert seen == ["public"] and "access denied" in body["result"]["error"]
    event["sessionAttributes"] = {"clearance": "confidential"}
    body = json.loads(
        ag.handler(event, None)["response"]["functionResponse"]["responseBody"]["TEXT"]["body"]
    )
    assert body["result"]["rows"] == [[1]] and body["cite_tables"] == [
        "gold.grid_reliability_daily"
    ]


def test_terraform_function_schema_matches_the_tools():
    committed = json.loads((REPO / "infra/terraform/agent_functions.json").read_text())
    assert committed == ag.function_schema(), (
        "regenerate: python -m gridlake.agent.bedrock_action_group"
    )
    assert all(len(f["parameters"]) <= 5 for f in committed)  # Bedrock Agents limit
