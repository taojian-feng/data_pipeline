"""Runtime configuration, resolved from environment variables.

Every setting has a local default so the full pipeline runs on a laptop with no cloud
credentials. On AWS the same code is configured purely through environment variables
(set by the Glue job / Step Functions task definitions in ``infra/terraform``).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

_PKG_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _PKG_DIR.parent.parent


def _resource_dir(name: str, packaged: str) -> Path:
    """Prefer the repo checkout; fall back to the copy bundled in the wheel."""
    repo_path = _REPO_ROOT / name
    return repo_path if repo_path.is_dir() else _PKG_DIR / packaged


@dataclass(frozen=True)
class Settings:
    home: Path = field(
        default_factory=lambda: Path(os.getenv("GRIDLAKE_HOME", ".gridlake")).resolve()
    )
    catalog_type: str = field(default_factory=lambda: os.getenv("GRIDLAKE_CATALOG", "local"))
    aws_region: str = field(default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"))
    glue_warehouse: str | None = field(default_factory=lambda: os.getenv("GRIDLAKE_WAREHOUSE"))
    s3_tables_bucket_arn: str | None = field(
        default_factory=lambda: os.getenv("GRIDLAKE_S3_TABLES_ARN")
    )
    eia_api_key: str | None = field(default_factory=lambda: os.getenv("EIA_API_KEY"))
    eia_api_key_secret: str | None = field(default_factory=lambda: os.getenv("EIA_API_KEY_SECRET"))
    kb_s3_uri: str | None = field(default_factory=lambda: os.getenv("GRIDLAKE_KB_S3_URI"))
    graph_s3_uri: str | None = field(default_factory=lambda: os.getenv("GRIDLAKE_GRAPH_S3_URI"))
    openlineage_url: str | None = field(default_factory=lambda: os.getenv("OPENLINEAGE_URL"))
    # Grid analyst agent: offline planner and local DuckDB by default, Bedrock + Athena on AWS.
    agent_llm: str = field(default_factory=lambda: os.getenv("GRIDLAKE_AGENT_LLM", "offline"))
    agent_model: str = field(
        default_factory=lambda: os.getenv("GRIDLAKE_AGENT_MODEL", "anthropic.claude-opus-5-5")
    )
    agent_sql: str = field(default_factory=lambda: os.getenv("GRIDLAKE_AGENT_SQL", "local"))
    athena_workgroup: str | None = field(
        default_factory=lambda: os.getenv("GRIDLAKE_ATHENA_WORKGROUP")
    )
    bedrock_kb_id: str | None = field(default_factory=lambda: os.getenv("GRIDLAKE_BEDROCK_KB_ID"))
    contracts_dir: Path = field(default_factory=lambda: _resource_dir("contracts", "_contracts"))
    seeds_dir: Path = field(default_factory=lambda: _resource_dir("seeds", "_seeds"))

    def resolve_eia_api_key(self) -> str | None:
        """API key from the environment, else from AWS Secrets Manager (on AWS)."""
        if self.eia_api_key or not self.eia_api_key_secret:
            return self.eia_api_key
        import boto3  # optional dependency, only needed on AWS

        client = boto3.client("secretsmanager", region_name=self.aws_region)
        return client.get_secret_value(SecretId=self.eia_api_key_secret)["SecretString"]

    @property
    def warehouse_dir(self) -> Path:
        return self.home / "warehouse"

    @property
    def lineage_dir(self) -> Path:
        return self.home / "lineage"

    @property
    def stream_dir(self) -> Path:
        return self.home / "stream"

    @property
    def knowledge_dir(self) -> Path:
        return self.home / "knowledge"

    @property
    def runs_dir(self) -> Path:
        return self.home / "runs"

    def ensure_dirs(self) -> None:
        for d in (
            self.warehouse_dir,
            self.lineage_dir,
            self.stream_dir,
            self.knowledge_dir,
            self.runs_dir,
        ):
            d.mkdir(parents=True, exist_ok=True)
