"""Grid analyst agent: answers questions by choosing access-aware tools, and cites them."""

from __future__ import annotations

from gridlake.agent.core import Answer, GridAnalyst
from gridlake.agent.llm import BedrockClaude, OfflinePlanner
from gridlake.agent.tools import Toolbox
from gridlake.config import Settings
from gridlake.knowledge.graph import KnowledgeGraph, Ontology

__all__ = ["Answer", "GridAnalyst", "Toolbox", "build_analyst", "build_toolbox", "load_graph"]


def load_graph(settings: Settings) -> KnowledgeGraph:
    """The published graph (S3 on AWS, ./.gridlake locally), else rebuilt from the lake."""
    if settings.graph_s3_uri:
        import boto3  # optional dependency, only needed on AWS

        bucket, _, key = settings.graph_s3_uri.removeprefix("s3://").partition("/")
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        return KnowledgeGraph.from_ntriples(Ontology.load(), body.decode())
    path = settings.knowledge_dir / "graph.nt"
    if path.exists():
        return KnowledgeGraph.from_ntriples(Ontology.load(), path.read_text())
    from gridlake.pipeline import Pipeline

    return Pipeline(settings).knowledge_graph()


def build_toolbox(settings: Settings, clearance: str) -> Toolbox:
    from gridlake.agent.tools import AthenaSql, BedrockKbRetriever, LocalRetriever, LocalSql
    from gridlake.contracts import load_contracts
    from gridlake.knowledge.retrieval import VectorIndex

    contracts = load_contracts(settings.contracts_dir)
    if settings.agent_sql == "athena":
        if not settings.athena_workgroup:
            raise ValueError("GRIDLAKE_ATHENA_WORKGROUP must be set when GRIDLAKE_AGENT_SQL=athena")
        sql = AthenaSql(settings.athena_workgroup, settings.aws_region)
    else:
        from gridlake.catalog import Lakehouse, get_catalog

        sql = LocalSql(Lakehouse(get_catalog(settings), contracts).read)
    if settings.bedrock_kb_id:
        retriever = BedrockKbRetriever(settings.bedrock_kb_id, settings.aws_region)
    else:
        retriever = LocalRetriever(lambda: VectorIndex.load(settings.knowledge_dir / "index"))
    return Toolbox(contracts, clearance, sql, retriever, graph=lambda: load_graph(settings))


def build_analyst(settings: Settings, clearance: str, llm: str | None = None) -> GridAnalyst:
    kind = llm or settings.agent_llm
    if kind == "bedrock":
        model = BedrockClaude(settings.agent_model, settings.aws_region)
    elif kind == "offline":
        model = OfflinePlanner()
    else:
        raise ValueError(f"unknown agent LLM {kind!r}; use offline or bedrock")
    return GridAnalyst(model, build_toolbox(settings, clearance))
