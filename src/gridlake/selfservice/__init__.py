"""Self-service data product agent: a person describes a dataset they need, and the agent
drafts the data contract, the SQL transformation and its tests as one patch, validated against
today's tables. A person approves it, which opens the pull request (``gridlake propose``,
``gridlake products``)."""

from __future__ import annotations

from pathlib import Path

from gridlake.config import Settings
from gridlake.reliability.drafts import DraftStore
from gridlake.selfservice.agent import Design, ProductDesigner, Request
from gridlake.selfservice.tools import ProductToolbox

__all__ = [
    "Design",
    "ProductDesigner",
    "ProductToolbox",
    "Request",
    "build_designer",
    "design_product",
    "proposal_store",
]


def proposal_store(settings: Settings) -> DraftStore:
    return DraftStore(settings.proposals_dir)


def build_designer(settings: Settings, llm: str | None = None) -> ProductDesigner:
    kind = llm or settings.agent_llm
    if kind == "bedrock":
        from gridlake.agent.llm import BedrockClaude

        return ProductDesigner(BedrockClaude(settings.agent_model, settings.aws_region))
    if kind == "offline":
        from gridlake.selfservice.planner import OfflineDesigner

        return ProductDesigner(OfflineDesigner())
    raise ValueError(f"unknown agent LLM {kind!r}; use offline or bedrock")


def design_product(
    settings: Settings,
    request: Request,
    clearance: str = "internal",
    llm: str | None = None,
    repo_root: Path | None = None,
) -> Design:
    """Run the agent on one request. Previews read the local lakehouse (or Athena)."""
    from gridlake.agent.tools import AthenaSql, LocalSql
    from gridlake.catalog import Lakehouse, get_catalog
    from gridlake.contracts import load_contracts

    contracts = load_contracts(settings.contracts_dir)
    lake = Lakehouse(get_catalog(settings), contracts)
    if settings.agent_sql == "athena" and settings.athena_workgroup:
        sql = AthenaSql(settings.athena_workgroup, settings.aws_region)
    else:
        sql = LocalSql(lake.read)
    toolbox = ProductToolbox(
        contracts,
        clearance,
        sql,
        lake.read,
        request.text,
        request.owner,
        proposal_store(settings),
        repo_root or settings.contracts_dir.parent,
        settings.contracts_dir,
        settings.transforms_dir,
    )
    return build_designer(settings, llm).design(request, toolbox)
