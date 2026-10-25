"""Grounded context packs for LLM agents (GraphRAG).

Given a question and the caller's clearance, assemble:
  1. entities recognised in the question (from knowledge-graph labels and codes),
  2. their graph facts (topology, generation mix, governing datasets),
  3. the top-k clearance-filtered passages from the vector index,
as a structured, citeable payload to place in a foundation model's prompt
(e.g. Amazon Bedrock) or to return from an agent tool.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from gridlake.knowledge.graph import KnowledgeGraph, Literal
from gridlake.knowledge.retrieval import VectorIndex

_DATE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
_LINKABLE = (
    "BalancingAuthority",
    "Region",
    "Interconnection",
    "EnergySource",
    "DataProduct",
    "BusinessTerm",
)


@dataclass
class ContextPack:
    question: str
    clearance: str
    entities: list[str]
    graph_facts: list[str]
    passages: list[dict[str, Any]]

    def to_prompt(self) -> str:
        facts = "\n".join(f"- {f}" for f in self.graph_facts) or "- (none)"
        passages = "\n".join(f"[{p['doc_id']}] {p['text']}" for p in self.passages) or "(none)"
        return (
            f"Answer using only the context below and cite passage ids.\n\n"
            f"## Knowledge graph facts\n{facts}\n\n## Retrieved passages\n{passages}\n\n"
            f"## Question\n{self.question}\n"
        )


def link_entities(graph: KnowledgeGraph, question: str) -> list[str]:
    q = question.lower()
    found = []
    for cls in _LINKABLE:
        for iri in graph.instances(cls):
            names = {graph.label(iri).lower()}
            names |= {
                str(o.value).lower()
                for prop in ("code", "altLabel")
                for o in graph.objects(iri, prop)
                if isinstance(o, Literal)
            }
            if any(len(n) >= 3 and re.search(rf"\b{re.escape(n)}\b", q) for n in names):
                found.append(iri)
    return found


def build_context(
    graph: KnowledgeGraph, index: VectorIndex, question: str, clearance: str = "public", k: int = 5
) -> ContextPack:
    entities = link_entities(graph, question)
    facts: list[str] = []
    for iri in entities:
        facts.extend(f for f in graph.describe(iri) if "has column" not in f)
    bas = [e for e in entities if graph.is_a(e, "BalancingAuthority")]
    codes = [o.value for o in graph.objects(bas[0], "code")] if len(bas) == 1 else []
    filters: dict[str, Any] = {"doc_type": "fact"}
    if codes:
        filters["ba_code"] = codes[0]
    if len(days := sorted(set(_DATE.findall(question)))) == 1:
        filters["date"] = days[0]
    hits = index.search(question, k=k, clearance=clearance, filters=filters)
    if not hits and "date" in filters:  # nothing for that exact day: fall back to the entity
        hits = index.search(
            question,
            k=k,
            clearance=clearance,
            filters={key: v for key, v in filters.items() if key != "date"},
        )
    # Always add the best catalog entries so the model knows which governed table backs the answer.
    hits += index.search(question, k=2, clearance=clearance, filters={"doc_type": "catalog"})
    return ContextPack(
        question=question,
        clearance=clearance,
        entities=[graph.label(e) for e in entities],
        graph_facts=list(dict.fromkeys(facts))[:40],
        passages=[
            {
                "doc_id": h.document.doc_id,
                "score": round(h.score, 4),
                "text": h.document.text,
                "classification": h.document.classification,
            }
            for h in hits
        ],
    )
