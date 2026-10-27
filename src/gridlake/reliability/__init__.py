"""Data reliability agent: when a run fails or a quality rule spikes, it reads the run log,
lineage and data contract, diagnoses the likely cause, and drafts a fix or a GitHub issue
that a person approves (``gridlake triage``, ``gridlake incidents``)."""

from __future__ import annotations

from gridlake.config import Settings
from gridlake.reliability.agent import ReliabilityAgent, Triage
from gridlake.reliability.drafts import Draft, DraftStore
from gridlake.reliability.runlog import (
    Incident,
    LocalRunLog,
    RunRecord,
    StepFunctionsRunLog,
    detect_incidents,
    quality_for_run,
)
from gridlake.reliability.tools import ReliabilityToolbox

__all__ = [
    "Draft",
    "DraftStore",
    "Incident",
    "ReliabilityAgent",
    "ReliabilityToolbox",
    "RunRecord",
    "Triage",
    "build_responder",
    "detect_incidents",
    "draft_store",
    "run_log",
    "triage_run",
]


def draft_store(settings: Settings) -> DraftStore:
    return DraftStore(settings.incidents_dir, settings.incidents_s3_uri)


def run_log(settings: Settings) -> LocalRunLog | StepFunctionsRunLog:
    if settings.state_machine_arn:
        return StepFunctionsRunLog(settings.state_machine_arn, settings.aws_region)
    return LocalRunLog(settings.runs_dir, settings.lineage_dir)


def lineage_edges(settings: Settings) -> list[tuple[str, str, str]]:
    """OpenLineage edges when this machine has the events, else the published graph's
    ``derivedFrom`` relations (on Lambda the events do not outlive an invocation)."""
    from gridlake import lineage as ol

    edges = ol.read_edges(settings.lineage_dir)
    if edges:
        return edges
    from gridlake.agent import load_graph

    graph = load_graph(settings)
    out = []
    for c in graph.instances("Dataset") + graph.instances("DataProduct"):
        for o in graph.objects(c, "derivedFrom"):
            out.append((graph.label(o), "", graph.label(c)))
    return sorted(set(out))


def build_responder(settings: Settings, llm: str | None = None) -> ReliabilityAgent:
    kind = llm or settings.agent_llm
    if kind == "bedrock":
        from gridlake.agent.llm import BedrockClaude

        return ReliabilityAgent(BedrockClaude(settings.agent_model, settings.aws_region))
    if kind == "offline":
        from gridlake.reliability.planner import OfflineResponder

        return ReliabilityAgent(OfflineResponder())
    raise ValueError(f"unknown agent LLM {kind!r}; use offline or bedrock")


def triage_run(
    settings: Settings,
    run_id: str | None = None,
    clearance: str = "internal",
    llm: str | None = None,
    min_rate: float = 0.02,
    min_rows: int = 5,
    log: LocalRunLog | StepFunctionsRunLog | None = None,
) -> tuple[RunRecord, list[Triage]]:
    """Detect incidents in one run (the latest by default) and triage each of them."""
    from gridlake.agent.tools import AthenaSql, LocalSql
    from gridlake.catalog import Lakehouse, get_catalog
    from gridlake.contracts import load_contracts

    log = log or run_log(settings)
    run = log.get(run_id) if run_id else log.latest()
    contracts = load_contracts(settings.contracts_dir)
    lake = Lakehouse(get_catalog(settings), contracts)
    incidents = detect_incidents(
        run, quality_for_run(lake.read("quality.results"), run), min_rate, min_rows
    )
    if not incidents:
        return run, []
    if settings.agent_sql == "athena" and settings.athena_workgroup:
        sql = AthenaSql(settings.athena_workgroup, settings.aws_region)
    else:
        sql = LocalSql(lake.read)
    edges: list = []

    def get_edges() -> list[tuple[str, str, str]]:
        if not edges:
            edges.extend(lineage_edges(settings))
        return edges

    agent, store = build_responder(settings, llm), draft_store(settings)
    triages = []
    for incident in incidents:
        toolbox = ReliabilityToolbox(
            contracts,
            clearance,
            sql,
            lake.read,
            incident,
            run,
            log,
            store,
            settings.contracts_dir,
            settings.seeds_dir,
            get_edges,
        )
        triages.append(agent.triage(incident, toolbox))
    return run, triages
