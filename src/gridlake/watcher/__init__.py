"""Grid anomaly watcher: a scheduled agent that spots unusual demand, interchange and fuel-mix
changes in the gold data products, explains them with context, and posts a daily brief
(``gridlake watch``)."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual

from gridlake.config import Settings
from gridlake.watcher.agent import AnomalyWatcher, Brief
from gridlake.watcher.context import TABLES, WatchData
from gridlake.watcher.detect import Anomaly, Detection, detect
from gridlake.watcher.tools import WatcherToolbox

__all__ = [
    "Anomaly",
    "AnomalyWatcher",
    "Brief",
    "BriefStore",
    "Detection",
    "WatcherToolbox",
    "build_watcher",
    "detect",
    "watch",
]


class BriefStore:
    """Markdown and JSON briefs in a local directory, or under an S3 prefix on AWS."""

    def __init__(self, directory: Path, s3_uri: str | None = None, client: Any = None):
        self.directory, self.s3_uri = directory, s3_uri
        if s3_uri:
            if client is None:
                import boto3  # optional dependency, only needed on AWS

                client = boto3.client("s3")
            self.client = client
            self.bucket, _, prefix = s3_uri.removeprefix("s3://").partition("/")
            self.prefix = prefix.rstrip("/") + "/" if prefix else ""

    def save(self, brief: Brief) -> str:
        """Write ``<date>.md`` and ``<date>.json``; returns where the Markdown went."""
        files = {
            f"{brief.date}.md": (brief.markdown(), "text/markdown"),
            f"{brief.date}.json": (
                json.dumps(brief.to_dict(), indent=2, default=str),
                "application/json",
            ),
        }
        if self.s3_uri:
            for name, (body, kind) in files.items():
                self.client.put_object(
                    Bucket=self.bucket,
                    Key=f"{self.prefix}{name}",
                    Body=body.encode(),
                    ContentType=kind,
                )
            return f"s3://{self.bucket}/{self.prefix}{brief.date}.md"
        self.directory.mkdir(parents=True, exist_ok=True)
        for name, (body, _) in files.items():
            (self.directory / name).write_text(body if body.endswith("\n") else body + "\n")
        return str(self.directory / f"{brief.date}.md")


def build_watcher(settings: Settings, llm: str | None = None) -> AnomalyWatcher:
    kind = llm or settings.agent_llm
    if kind == "bedrock":
        from gridlake.agent.llm import BedrockClaude

        return AnomalyWatcher(BedrockClaude(settings.agent_model, settings.aws_region))
    if kind == "offline":
        from gridlake.watcher.planner import OfflineBriefWriter

        return AnomalyWatcher(OfflineBriefWriter())
    raise ValueError(f"unknown agent LLM {kind!r}; use offline or bedrock")


def watch(
    settings: Settings,
    day: date | None = None,
    llm: str | None = None,
    clearance: str = "public",
    lookback: int = 28,
    z_min: float = 3.5,
) -> Brief:
    """Detect anomalies on ``day`` (default: the latest complete UTC day in gold) and write
    the brief. Nothing is saved or sent here; see ``BriefStore`` and ``announce``."""
    from gridlake.agent import load_graph
    from gridlake.agent.tools import AccessDenied, AthenaSql, LocalSql
    from gridlake.catalog import Lakehouse, get_catalog
    from gridlake.contracts import cleared, load_contracts

    contracts = load_contracts(settings.contracts_dir)
    for table in TABLES:
        if not cleared(contracts[table].classification, clearance):
            raise AccessDenied(
                f"{table} is {contracts[table].classification}; clearance is {clearance}"
            )
    lake = Lakehouse(get_catalog(settings), contracts)
    if day is None:
        yesterday = datetime.now(UTC).date() - timedelta(days=1)
        recent = lake.read(
            "gold.ba_daily_energy",
            GreaterThanOrEqual("date", (yesterday - timedelta(days=lookback)).isoformat()),
        )
        days = [d for d in recent.column("date").to_pylist() if d <= yesterday]
        day = max(days) if days else yesterday
    window = GreaterThanOrEqual("date", (day - timedelta(days=lookback + 1)).isoformat())
    window = window & LessThanOrEqual("date", day.isoformat())
    daily = lake.read("gold.ba_daily_energy", window).to_pylist()
    fuel_mix = lake.read("gold.fuel_mix_daily", window).to_pylist()
    detection = detect(daily, fuel_mix, day, lookback=lookback, z_min=z_min)
    if settings.agent_sql == "athena" and settings.athena_workgroup:
        sql = AthenaSql(settings.athena_workgroup, settings.aws_region)
    else:
        sql = LocalSql(lake.read)
    toolbox = WatcherToolbox(
        contracts,
        clearance,
        sql,
        WatchData(daily, fuel_mix, detection, lookback),
        graph=lambda: load_graph(settings),
    )
    return build_watcher(settings, llm).brief(detection, toolbox)


def announce(settings: Settings, brief: Brief, location: str, sns: Any = None) -> bool:
    """Post the brief to the brief topic (email, chat or ticketing subscribers), if one is set."""
    if not settings.brief_topic_arn:
        return False
    if sns is None:
        import boto3  # optional dependency, only needed on AWS

        sns = boto3.client("sns", region_name=settings.aws_region)
    body = brief.markdown() + f"\nFull brief: {location}\n"
    sns.publish(
        TopicArn=settings.brief_topic_arn, Subject=brief.title[:100], Message=body[:250_000]
    )
    return True
