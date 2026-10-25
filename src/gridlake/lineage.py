"""OpenLineage emitter.

Every pipeline step emits START and COMPLETE/FAIL RunEvents (OpenLineage 2-0-2 spec)
with schema and data-quality facets on the datasets it touched. Events are written to
JSONL locally and, when ``OPENLINEAGE_URL`` is set, POSTed to any OpenLineage backend
(Marquez, Amazon DataZone / SageMaker Catalog lineage, DataHub, ...).
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from gridlake.contracts import Contract

log = logging.getLogger(__name__)

PRODUCER = "https://github.com/taojian-feng/data_pipeline"
SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
NAMESPACE = "gridlake"


def _facet(name: str) -> dict[str, str]:
    return {
        "_producer": PRODUCER,
        "_schemaURL": f"https://openlineage.io/spec/facets/1-0-0/{name}.json",
    }


def dataset(
    contract: Contract, quality: dict[str, Any] | None = None, snapshot_id: int | None = None
) -> dict:
    facets: dict[str, Any] = {
        "schema": {
            **_facet("SchemaDatasetFacet"),
            "fields": [
                {"name": c.name, "type": c.type, "description": c.description}
                for c in contract.columns
            ],
        },
        "ownership": {
            **_facet("OwnershipDatasetFacet"),
            "owners": [{"name": contract.owner, "type": "team"}],
        },
        "documentation": {
            **_facet("DocumentationDatasetFacet"),
            "description": contract.description,
        },
    }
    if snapshot_id is not None:
        facets["version"] = {
            **_facet("DatasetVersionDatasetFacet"),
            "datasetVersion": str(snapshot_id),
        }
    ds: dict[str, Any] = {"namespace": NAMESPACE, "name": contract.name, "facets": facets}
    if quality:
        ds["inputFacets" if quality.get("_input") else "outputFacets"] = {
            "dataQualityMetrics": {
                **_facet("DataQualityMetricsInputDatasetFacet"),
                "rowCount": quality["rowCount"],
            },
            "dataQualityAssertions": {
                **_facet("DataQualityAssertionsDatasetFacet"),
                "assertions": quality["assertions"],
            },
        }
    return ds


@dataclass
class LineageEmitter:
    directory: Path
    url: str | None = None
    parent_run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    events: list[dict] = field(default_factory=list)

    def emit(
        self,
        event_type: str,
        job: str,
        run_id: str,
        inputs: list[dict],
        outputs: list[dict],
        error: str | None = None,
    ) -> dict:
        run_facets: dict[str, Any] = {
            "parent": {
                **_facet("ParentRunFacet"),
                "run": {"runId": self.parent_run_id},
                "job": {"namespace": NAMESPACE, "name": "gridlake_pipeline"},
            },
        }
        if error:
            run_facets["errorMessage"] = {
                **_facet("ErrorMessageRunFacet"),
                "message": error,
                "programmingLanguage": "python",
            }
        event = {
            "eventType": event_type,
            "eventTime": datetime.now(UTC).isoformat(),
            "producer": PRODUCER,
            "schemaURL": SCHEMA_URL,
            "run": {"runId": run_id, "facets": run_facets},
            "job": {
                "namespace": NAMESPACE,
                "name": job,
                "facets": {
                    "jobType": {
                        **_facet("JobTypeJobFacet"),
                        "processingType": "BATCH",
                        "integration": "GRIDLAKE",
                        "jobType": "TASK",
                    }
                },
            },
            "inputs": inputs,
            "outputs": outputs,
        }
        self.events.append(event)
        self.directory.mkdir(parents=True, exist_ok=True)
        with (self.directory / f"{self.parent_run_id}.jsonl").open("a") as f:
            f.write(json.dumps(event, default=str) + "\n")
        if self.url:
            try:
                requests.post(f"{self.url.rstrip('/')}/api/v1/lineage", json=event, timeout=5)
            except requests.RequestException as exc:  # lineage must never break the pipeline
                log.warning("OpenLineage POST failed: %s", exc)
        return event

    def edges(self) -> list[tuple[str, str, str]]:
        """(input_dataset, job, output_dataset) triples from COMPLETE events."""
        out = []
        for e in self.events:
            if e["eventType"] != "COMPLETE":
                continue
            for i in e["inputs"]:
                for o in e["outputs"]:
                    out.append((i["name"], e["job"]["name"], o["name"]))
        return out


def read_edges(directory: Path) -> list[tuple[str, str, str]]:
    edges: set[tuple[str, str, str]] = set()
    for path in directory.glob("*.jsonl"):
        for line in path.read_text().splitlines():
            e = json.loads(line)
            if e["eventType"] == "COMPLETE":
                for i in e["inputs"]:
                    for o in e["outputs"]:
                        edges.add((i["name"], e["job"]["name"], o["name"]))
    return sorted(edges)
