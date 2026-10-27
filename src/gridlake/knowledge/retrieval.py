"""Access-aware retrieval for RAG and agents.

Documents are generated from gold data products and from data contracts (so agents can
answer both "what was CISO's renewable share last Tuesday?" and "which table holds
forecast accuracy, and who owns it?").

Every document carries metadata, including its source table's classification. Search
enforces the caller's clearance *before* ranking, mirroring Lake Formation / Bedrock
Knowledge Base metadata filtering, so restricted content can never reach a prompt.

The default embedder is a deterministic hashed TF-IDF so everything runs offline; on
AWS, ``export_bedrock_kb`` writes the same documents with ``.metadata.json`` sidecars
for an Amazon Bedrock Knowledge Base data source.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import math
import re
import zlib
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Protocol

import numpy as np

from gridlake.contracts import CLASSIFICATIONS, Contract, metadata_classification

_TOKEN = re.compile(r"[a-z0-9]+(?:\.[a-z0-9_]+)*")


@dataclass
class Document:
    doc_id: str
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def classification(self) -> str:
        return self.metadata.get("classification", "confidential")


@dataclass
class Hit:
    document: Document
    score: float


def _stem(tok: str) -> str:
    """Tiny plural stemmer: authorities -> authority, renewables -> renewable."""
    if len(tok) > 4 and tok.endswith("ies"):
        return tok[:-3] + "y"
    if len(tok) > 3 and tok.endswith("s") and not tok.endswith("ss"):
        return tok[:-1]
    return tok


def tokenize(text: str) -> list[str]:
    toks = [_stem(t) for t in _TOKEN.findall(text.lower())]
    return toks + [f"{a}_{b}" for a, b in itertools.pairwise(toks)]


class Embedder(Protocol):
    def fit(self, texts: Sequence[str]) -> None: ...

    def embed(self, texts: Sequence[str]) -> np.ndarray: ...


class HashingTfidfEmbedder:
    """Feature-hashed TF-IDF: no vocabulary to store, deterministic across processes."""

    def __init__(self, dim: int = 2**14):
        self.dim = dim
        self.idf = np.ones(dim, dtype=np.float32)

    def _counts(self, text: str) -> np.ndarray:
        v = np.zeros(self.dim, dtype=np.float32)
        for tok in tokenize(text):
            v[zlib.crc32(tok.encode()) % self.dim] += 1.0
        return v

    def fit(self, texts: Sequence[str]) -> None:
        df = np.zeros(self.dim, dtype=np.float32)
        for t in texts:
            df += self._counts(t) > 0
        self.idf = np.log((1 + len(texts)) / (1 + df)).astype(np.float32) + 1.0

    def embed(self, texts: Sequence[str]) -> np.ndarray:
        m = (
            np.stack([np.log1p(self._counts(t)) * self.idf for t in texts])
            if texts
            else np.zeros((0, self.dim))
        )
        norms = np.linalg.norm(m, axis=1, keepdims=True)
        return m / np.where(norms == 0, 1, norms)


class BedrockEmbedder:
    """Amazon Titan Text Embeddings V2 via Bedrock (requires the ``aws`` extra)."""

    def __init__(self, model_id: str = "amazon.titan-embed-text-v2:0", region: str = "us-east-1"):
        import boto3  # lazy: optional dependency

        self.client = boto3.client("bedrock-runtime", region_name=region)
        self.model_id = model_id

    def fit(self, texts: Sequence[str]) -> None:
        return None

    def embed(self, texts: Sequence[str]) -> np.ndarray:
        vecs = []
        for t in texts:
            body = json.dumps({"inputText": t, "normalize": True})
            resp = self.client.invoke_model(modelId=self.model_id, body=body)
            vecs.append(json.loads(resp["body"].read())["embedding"])
        return np.array(vecs, dtype=np.float32)


def allowed_classifications(clearance: str) -> set[str]:
    """Clearance is hierarchical: confidential > internal > public."""
    return set(CLASSIFICATIONS[: CLASSIFICATIONS.index(clearance) + 1])


class VectorIndex:
    def __init__(self, embedder: Embedder | None = None):
        self.embedder = embedder or HashingTfidfEmbedder()
        self.docs: list[Document] = []
        self.vectors = np.zeros((0, 1), dtype=np.float32)

    def build(self, docs: Iterable[Document]) -> VectorIndex:
        self.docs = list(docs)
        texts = [d.text for d in self.docs]
        self.embedder.fit(texts)
        self.vectors = self.embedder.embed(texts)
        return self

    def search(
        self,
        query: str,
        k: int = 5,
        clearance: str = "public",
        filters: dict[str, Any] | None = None,
    ) -> list[Hit]:
        allowed = allowed_classifications(clearance)
        mask = np.array(
            [
                d.classification in allowed
                and all(d.metadata.get(key) == val for key, val in (filters or {}).items())
                for d in self.docs
            ],
            dtype=bool,
        )
        if not mask.any():
            return []
        scores = self.vectors @ self.embedder.embed([query])[0]
        scores = np.where(mask, scores, -math.inf)
        top = np.argsort(-scores)[:k]
        return [
            Hit(self.docs[i], float(scores[i]))
            for i in top
            if np.isfinite(scores[i]) and scores[i] > 0
        ]

    def save(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            directory / "index.npz",
            vectors=self.vectors,
            idf=getattr(self.embedder, "idf", np.zeros(0)),
        )
        with (directory / "documents.jsonl").open("w") as f:
            for d in self.docs:
                f.write(json.dumps(asdict(d), default=str) + "\n")

    @classmethod
    def load(cls, directory: Path) -> VectorIndex:
        idx = cls()
        data = np.load(directory / "index.npz")
        idx.vectors = data["vectors"]
        if isinstance(idx.embedder, HashingTfidfEmbedder) and data["idf"].size:
            idx.embedder.idf = data["idf"]
        idx.docs = [
            Document(**json.loads(line))
            for line in (directory / "documents.jsonl").read_text().splitlines()
        ]
        return idx


# -- document generation -----------------------------------------------------


def _fmt(v: float | None, unit: str = "", digits: int = 0) -> str:
    return "n/a" if v is None else f"{v:,.{digits}f}{unit}"


def daily_energy_documents(rows: Iterable[dict], classification: str) -> list[Document]:
    docs = []
    for r in rows:
        text = (
            f"On {r['date']}, {r['ba_name']} ({r['ba_code']}), a balancing authority in the "
            f"{r['region_name']} region of the {r['interconnection']} Interconnection, served "
            f"{_fmt(r['total_demand_mwh'], ' MWh')} of demand with a peak of {_fmt(r['peak_demand_mwh'], ' MWh')} "
            f"at {r['peak_hour_utc']:02d}:00 UTC. The day-ahead forecast error (MAPE) was "
            f"{_fmt(r['forecast_mape_pct'], '%', 2)}. Renewables (solar, wind, hydro) supplied "
            f"{_fmt(r['renewable_share_pct'], '%', 1)} of generation; carbon intensity was "
            f"{_fmt(r['carbon_intensity_kg_per_mwh'], ' kg CO2/MWh', 0)} and emissions "
            f"{_fmt(r['total_emissions_t'], ' t CO2', 0)}. Hours reported: {r['hours_reported']}."
        )
        docs.append(
            Document(
                doc_id=f"ba_daily_energy/{r['ba_code']}/{r['date']}",
                text=text,
                metadata={
                    "source_table": "gold.ba_daily_energy",
                    "ba_code": r["ba_code"],
                    "date": str(r["date"]),
                    "region": r["region_name"],
                    "interconnection": r["interconnection"],
                    "classification": classification,
                    "doc_type": "fact",
                },
            )
        )
    return docs


_RANKINGS = (
    ("renewable_share_pct", "average renewable share (highest first)", "%", 1, True),
    (
        "carbon_intensity_kg_per_mwh",
        "average carbon intensity (lowest first)",
        " kg CO2/MWh",
        0,
        False,
    ),
    (
        "forecast_mape_pct",
        "average day-ahead forecast error MAPE (most accurate first)",
        "%",
        2,
        False,
    ),
    ("peak_demand_mwh", "average daily peak demand (largest first)", " MWh", 0, True),
)


def summary_documents(rows: Iterable[dict], classification: str) -> list[Document]:
    """Pre-aggregated rankings across BAs.

    Retrieval over per-day facts cannot answer superlatives ("which BA had the highest...?");
    materialising the comparison as its own document can.
    """
    rows = list(rows)
    if not rows:
        return []
    window = f"{min(r['date'] for r in rows)} to {max(r['date'] for r in rows)}"
    by_ba: dict[str, list[dict]] = {}
    for r in rows:
        by_ba.setdefault(r["ba_code"], []).append(r)
    names = {code: rs[0]["ba_name"] for code, rs in by_ba.items()}
    docs = []
    for col, title, unit, digits, desc in _RANKINGS:
        avg = {code: sum(r[col] or 0 for r in rs) / len(rs) for code, rs in by_ba.items()}
        ranked = sorted(avg.items(), key=lambda kv: -kv[1] if desc else kv[1])
        listing = "; ".join(
            f"{i}. {names[c]} ({c}) {_fmt(v, unit, digits)}" for i, (c, v) in enumerate(ranked, 1)
        )
        docs.append(
            Document(
                doc_id=f"ranking/{col}",
                text=f"Ranking of balancing authorities by {title}, {window}: {listing}.",
                metadata={
                    "source_table": "gold.ba_daily_energy",
                    "classification": classification,
                    "doc_type": "fact",
                    "window": window,
                },
            )
        )
    return docs


def reliability_documents(rows: Iterable[dict], classification: str) -> list[Document]:
    return [
        Document(
            doc_id=f"grid_reliability_daily/{r['ba_code']}/{r['date']}",
            text=(
                f"On {r['date']}, {r['substations_reporting']} substations in balancing authority {r['ba_code']} "
                f"reported {r['minutes_observed']} substation-minutes of telemetry, with "
                f"{r['excursion_minutes']} minutes outside the 59.95-60.05 Hz frequency band. "
                f"Lowest frequency {_fmt(r['min_frequency_hz'], ' Hz', 3)}, highest "
                f"{_fmt(r['max_frequency_hz'], ' Hz', 3)}; maximum substation load {_fmt(r['max_load_mw'], ' MW', 1)}."
            ),
            metadata={
                "source_table": "gold.grid_reliability_daily",
                "ba_code": r["ba_code"],
                "date": str(r["date"]),
                "classification": classification,
                "doc_type": "fact",
            },
        )
        for r in rows
    ]


def catalog_documents(
    contracts: Iterable[Contract], lineage: dict[str, list[str]]
) -> list[Document]:
    docs = []
    for c in contracts:
        cols = "; ".join(
            f"{col.name} ({col.type}{', ' + col.business_term if col.business_term else ''})"
            + (f": {col.description}" if col.description else "")
            for col in c.columns
        )
        rules = ", ".join(
            r["rule"]
            + (
                f"({r.get('column') or ','.join(r.get('columns', []))})"
                if r.get("column") or r.get("columns")
                else ""
            )
            for r in c.quality
        )
        upstream = ", ".join(lineage.get(c.name, [])) or "none recorded"
        text = (
            f"Table {c.name} is a {c.layer}-layer dataset in the {c.domain} domain, owned by {c.owner}, "
            f"classified {c.classification}. {c.description} Columns: {cols}. "
            f"Primary key: {', '.join(c.primary_key) or 'none'}. Quality rules: {rules or 'none'}. "
            f"Derived from: {upstream}. Consumers: {', '.join(c.consumers) or 'none'}."
        )
        docs.append(
            Document(
                doc_id=f"catalog/{c.name}",
                text=text,
                metadata={
                    "source_table": c.name,
                    "layer": c.layer,
                    "owner": c.owner,
                    "classification": metadata_classification(c.classification),
                    "doc_type": "catalog",
                },
            )
        )
    return docs


def upload_to_s3(directory: Path, s3_uri: str) -> int:
    """Publish an exported KB folder to the S3 prefix a Bedrock data source points at."""
    import boto3  # optional dependency, only needed on AWS

    bucket, _, prefix = s3_uri.removeprefix("s3://").partition("/")
    s3 = boto3.client("s3")
    files = sorted(p for p in directory.iterdir() if p.is_file())
    for p in files:
        s3.upload_file(str(p), bucket, f"{prefix.rstrip('/')}/{p.name}".lstrip("/"))
    return len(files)


def upload_file_to_s3(path: Path, s3_uri: str) -> None:
    import boto3  # optional dependency, only needed on AWS

    bucket, _, key = s3_uri.removeprefix("s3://").partition("/")
    boto3.client("s3").upload_file(str(path), bucket, key)


def export_bedrock_kb(docs: Iterable[Document], directory: Path) -> int:
    """Write an S3-ready Bedrock Knowledge Base data source (text + metadata sidecars)."""
    directory.mkdir(parents=True, exist_ok=True)
    n = 0
    for d in docs:
        stem = hashlib.sha1(d.doc_id.encode()).hexdigest()[:16]
        (directory / f"{stem}.txt").write_text(d.text)
        attrs = {k: v for k, v in {**d.metadata, "doc_id": d.doc_id}.items() if v is not None}
        (directory / f"{stem}.txt.metadata.json").write_text(
            json.dumps({"metadataAttributes": attrs}, default=str)
        )
        n += 1
    return n
