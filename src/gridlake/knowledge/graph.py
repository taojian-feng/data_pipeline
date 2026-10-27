"""A small, dependency-free RDF knowledge graph with ontology validation.

The graph joins two worlds that agents need together:
  * domain knowledge - balancing authorities, regions, interconnections, energy sources,
    derived facts such as each BA's generation mix (from gold data products);
  * the data estate - datasets, columns, owners, classifications, business terms and
    lineage (from data contracts and OpenLineage events).

Exports Turtle and N-Triples, which load directly into Amazon Neptune (bulk loader)
or any triple store; the ontology itself exports as OWL.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from gridlake.contracts import cleared, metadata_classification

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
OWL = "http://www.w3.org/2002/07/owl#"
XSD = "http://www.w3.org/2001/XMLSchema#"
RDFS_LABEL = RDFS + "label"
RDFS_COMMENT = RDFS + "comment"
_XSD_TYPES = {"string": "string", "boolean": "boolean", "decimal": "decimal"}
_NT_LINE = re.compile(r'<([^>]*)> <([^>]*)> (?:<([^>]*)>|"((?:[^"\\]|\\.)*)"(?:\^\^<([^>]*)>)?) \.')


class OntologyViolation(ValueError):
    pass


@dataclass(frozen=True)
class Literal:
    value: Any
    datatype: str = "string"

    def n3(self) -> str:
        if self.datatype == "boolean":
            return f'"{str(bool(self.value)).lower()}"^^<{XSD}boolean>'
        if self.datatype == "decimal":
            return f'"{self.value}"^^<{XSD}decimal>'
        escaped = str(self.value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        return f'"{escaped}"'


Node = str | Literal


class Ontology:
    def __init__(self, spec: dict[str, Any]):
        self.ns: str = spec["namespace"]
        self.prefix: str = spec["prefix"]
        self.classes: dict[str, dict] = spec["classes"]
        self.object_properties: dict[str, dict] = spec["object_properties"]
        self.datatype_properties: dict[str, dict] = spec["datatype_properties"]

    @classmethod
    def load(cls, path: Path | None = None) -> Ontology:
        path = path or Path(__file__).with_name("ontology.yaml")
        return cls(yaml.safe_load(path.read_text()))

    def iri(self, local: str) -> str:
        return self.ns + local

    def local(self, iri: str) -> str:
        return iri[len(self.ns) :] if iri.startswith(self.ns) else iri

    def superclasses(self, cls: str) -> set[str]:
        out = {cls}
        while (parent := self.classes.get(cls, {}).get("subClassOf")) is not None:
            out.add(parent)
            cls = parent
        return out

    def to_turtle(self) -> str:
        lines = [
            f"@prefix {self.prefix}: <{self.ns}> .",
            f"@prefix owl: <{OWL}> .",
            f"@prefix rdfs: <{RDFS}> .",
            f"@prefix xsd: <{XSD}> .",
            "",
        ]
        for name, c in self.classes.items():
            lines.append(f"{self.prefix}:{name} a owl:Class")
            if "subClassOf" in c:
                lines.append(f"    ; rdfs:subClassOf {self.prefix}:{c['subClassOf']}")
            if "comment" in c:
                lines.append(f"    ; rdfs:comment {Literal(c['comment']).n3()}")
            lines[-1] += " ."
        for name, p in self.object_properties.items():
            rng = "owl:Class" if p["range"] == "Class" else f"{self.prefix}:{p['range']}"
            lines.append(
                f"{self.prefix}:{name} a owl:ObjectProperty ; rdfs:domain {self.prefix}:{p['domain']} ; rdfs:range {rng} ."
            )
        for name, p in self.datatype_properties.items():
            lines.append(
                f"{self.prefix}:{name} a owl:DatatypeProperty ; rdfs:domain {self.prefix}:{p['domain']} ; rdfs:range xsd:{_XSD_TYPES[p['range']]} ."
            )
        return "\n".join(lines) + "\n"


class KnowledgeGraph:
    def __init__(self, ontology: Ontology):
        self.ont = ontology
        self.triples: set[tuple[str, str, Node]] = set()
        self._types: dict[str, set[str]] = defaultdict(set)

    # -- construction -------------------------------------------------------
    def entity(self, kind: str, key: str) -> str:
        return self.ont.iri(f"{kind}/{re.sub(r'[^A-Za-z0-9._-]', '_', key)}")

    def add_entity(self, kind: str, key: str, label: str, comment: str | None = None) -> str:
        if kind not in self.ont.classes:
            raise OntologyViolation(f"unknown class {kind!r}")
        iri = self.entity(kind, key)
        self.triples.add((iri, RDF_TYPE, self.ont.iri(kind)))
        self.triples.add((iri, RDFS_LABEL, Literal(label)))
        if comment:
            self.triples.add((iri, RDFS_COMMENT, Literal(comment)))
        self._types[iri].add(kind)
        return iri

    def is_a(self, iri: str, cls: str) -> bool:
        return any(cls in self.ont.superclasses(t) for t in self._types.get(iri, ()))

    def relate(self, subject: str, prop: str, obj: str) -> None:
        spec = self.ont.object_properties.get(prop)
        if spec is None:
            raise OntologyViolation(f"unknown object property {prop!r}")
        if not self.is_a(subject, spec["domain"]):
            raise OntologyViolation(
                f"{prop}: subject {self.ont.local(subject)} is not a {spec['domain']}"
            )
        if spec["range"] == "Class":
            if self.ont.local(obj) not in self.ont.classes:
                raise OntologyViolation(f"{prop}: {obj} is not an ontology class")
        elif not self.is_a(obj, spec["range"]):
            raise OntologyViolation(
                f"{prop}: object {self.ont.local(obj)} is not a {spec['range']}"
            )
        self.triples.add((subject, self.ont.iri(prop), obj))

    def set(self, subject: str, prop: str, value: Any) -> None:
        spec = self.ont.datatype_properties.get(prop)
        if spec is None:
            raise OntologyViolation(f"unknown datatype property {prop!r}")
        if not self.is_a(subject, spec["domain"]):
            raise OntologyViolation(
                f"{prop}: subject {self.ont.local(subject)} is not a {spec['domain']}"
            )
        if "values" in spec and value not in spec["values"]:
            raise OntologyViolation(f"{prop}: {value!r} not in {spec['values']}")
        if spec["range"] == "boolean" and not isinstance(value, bool):
            raise OntologyViolation(f"{prop}: expected boolean, got {value!r}")
        if spec["range"] == "decimal" and not isinstance(value, int | float):
            raise OntologyViolation(f"{prop}: expected number, got {value!r}")
        self.triples.add((subject, self.ont.iri(prop), Literal(value, spec["range"])))

    # -- querying -----------------------------------------------------------
    def match(
        self, s: str | None = None, p: str | None = None, o: Node | None = None
    ) -> Iterator[tuple[str, str, Node]]:
        for t in self.triples:
            if (s is None or t[0] == s) and (p is None or t[1] == p) and (o is None or t[2] == o):
                yield t

    def objects(self, s: str, prop: str) -> list[Node]:
        p = prop if prop.startswith("http") else self.ont.iri(prop)
        return sorted((o for _, _, o in self.match(s, p)), key=str)

    def label(self, iri: str) -> str:
        labels = self.objects(iri, RDFS_LABEL)
        return labels[0].value if labels else self.ont.local(iri)

    def instances(self, cls: str) -> list[str]:
        return sorted(iri for iri in self._types if self.is_a(iri, cls))

    def visible(self, iri: str, clearance: str) -> bool:
        """Can a caller with ``clearance`` see this node? Grid entities are open; datasets
        and their columns follow the dataset's metadata classification (ADR 0005)."""
        if self.is_a(iri, "Substation"):  # known only from confidential telemetry
            return cleared("confidential", clearance)
        if self.is_a(iri, "Column"):
            return all(
                self.visible(ds, clearance)
                for ds, _, _ in self.match(p=self.ont.iri("hasColumn"), o=iri)
            )
        if self.is_a(iri, "Dataset"):
            cls = [o.value for o in self.objects(iri, "classification") if isinstance(o, Literal)]
            return cleared(metadata_classification(cls[0]) if cls else None, clearance)
        return True

    def describe(self, iri: str, clearance: str | None = None) -> list[str]:
        """Human/LLM-readable facts about an entity, outgoing and incoming. With a clearance,
        facts touching a dataset or column above it are left out."""
        if clearance is not None and not self.visible(iri, clearance):
            return []

        def shown(node: Node) -> bool:
            return clearance is None or isinstance(node, Literal) or self.visible(node, clearance)

        facts = []
        name = self.label(iri)
        for _, p, o in sorted(self.match(s=iri), key=str):
            if p == RDF_TYPE:
                facts.append(f"{name} is a {self.ont.local(o)}")
            elif p in (RDFS_LABEL,) or not shown(o):
                continue
            elif isinstance(o, Literal):
                facts.append(f"{name} {self._pname(p)} {o.value}")
            else:
                facts.append(f"{name} {self._pname(p)} {self.label(o)}")
        for s, p, _ in sorted(self.match(o=iri), key=str):
            if shown(s):
                facts.append(f"{self.label(s)} {self._pname(p)} {name}")
        return facts

    def upstream(self, dataset_iri: str) -> list[str]:
        """Transitive lineage: every dataset this one is derived from."""
        seen: list[str] = []
        frontier = [dataset_iri]
        while frontier:
            node = frontier.pop()
            for o in self.objects(node, "derivedFrom"):
                if o not in seen:
                    seen.append(o)
                    frontier.append(o)
        return seen

    def _pname(self, iri: str) -> str:
        if iri.startswith(self.ont.ns):
            return re.sub(r"(?<=[a-z])([A-Z])", r" \1", self.ont.local(iri)).lower()
        return iri.rsplit("#", 1)[-1]

    # -- export -------------------------------------------------------------
    @staticmethod
    def _n3(node: Node) -> str:
        return node.n3() if isinstance(node, Literal) else f"<{node}>"

    def to_ntriples(self) -> str:
        return "".join(
            f"<{s}> <{p}> {self._n3(o)} .\n" for s, p, o in sorted(self.triples, key=str)
        )

    @classmethod
    def from_ntriples(cls, ontology: Ontology, text: str) -> KnowledgeGraph:
        """Load what ``to_ntriples`` wrote (e.g. the graph.nt the pipeline publishes to S3)."""
        g = cls(ontology)
        for line in filter(None, (ln.strip() for ln in text.splitlines())):
            m = _NT_LINE.fullmatch(line)
            if m is None:
                raise ValueError(f"not an N-Triples line: {line[:120]}")
            s, p, iri, lit, dtype = m.groups()
            if iri is not None:
                o: Node = iri
                if p == RDF_TYPE:
                    g._types[s].add(ontology.local(iri))
            else:
                raw = re.sub(r'\\(["\\n])', lambda e: "\n" if e[1] == "n" else e[1], lit)
                kind = (dtype or "").rsplit("#", 1)[-1] or "string"
                if kind == "boolean":
                    o = Literal(raw == "true", "boolean")
                elif kind == "decimal":
                    o = Literal(float(raw), "decimal")
                else:
                    o = Literal(raw)
            g.triples.add((s, p, o))
        return g

    def to_turtle(self) -> str:
        ns, pre = self.ont.ns, self.ont.prefix

        def short(n: Node) -> str:
            if isinstance(n, Literal):
                return (
                    n.n3().replace(f"<{XSD}", "xsd:").replace(">", "")
                    if n.datatype != "string"
                    else n.n3()
                )
            if n == RDF_TYPE:
                return "a"
            if n.startswith(RDFS):
                return "rdfs:" + n[len(RDFS) :]
            if n.startswith(ns) and "/" not in n[len(ns) :]:
                return f"{pre}:{n[len(ns) :]}"
            return f"<{n}>"

        by_subject: dict[str, list[tuple[str, Node]]] = defaultdict(list)
        for s, p, o in self.triples:
            by_subject[s].append((p, o))
        out = [
            f"@prefix {pre}: <{ns}> .",
            f"@prefix rdfs: <{RDFS}> .",
            f"@prefix xsd: <{XSD}> .",
            "",
        ]
        for s in sorted(by_subject):
            preds = sorted(by_subject[s], key=lambda po: (po[0] != RDF_TYPE, str(po)))
            body = " ;\n    ".join(f"{short(p)} {short(o)}" for p, o in preds)
            out.append(f"<{s}>\n    {body} .\n")
        return "\n".join(out)

    def stats(self) -> dict[str, int]:
        counts: dict[str, int] = defaultdict(int)
        for types in self._types.values():
            for t in types:
                counts[t] += 1
        return {"triples": len(self.triples), **dict(sorted(counts.items()))}


def build_graph(
    ontology: Ontology,
    dim_ba: Iterable[dict],
    dim_fuel: Iterable[dict],
    contracts: Iterable[Any],
    lineage_edges: Iterable[tuple[str, str, str]],
    fuel_mix: Iterable[dict] = (),
    daily_energy: Iterable[dict] = (),
    substations: Iterable[tuple[str, str]] = (),
) -> KnowledgeGraph:
    g = KnowledgeGraph(ontology)
    contracts = list(contracts)

    # Domain: grid topology
    for f in dim_fuel:
        src = g.add_entity("EnergySource", f["fuel_type"], f["fuel_name"])
        cat = g.add_entity("FuelCategory", f["fuel_category"], f["fuel_category"])
        g.relate(src, "hasFuelCategory", cat)
        g.set(src, "isRenewable", bool(f["is_renewable"]))
        g.set(src, "emissionFactorKgPerMWh", float(f["emission_factor_kg_per_mwh"]))
    for b in dim_ba:
        ba = g.add_entity("BalancingAuthority", b["ba_code"], b["ba_name"])
        g.set(ba, "code", b["ba_code"])
        g.set(ba, "timezone", b["timezone"])
        for alias in filter(None, (b.get("aliases") or "").split("|")):
            g.set(ba, "altLabel", alias)
        region = g.add_entity("Region", b["region_code"], b["region_name"])
        g.set(region, "code", b["region_code"])
        g.relate(ba, "operatesInRegion", region)
        g.relate(
            ba,
            "partOfInterconnection",
            g.add_entity(
                "Interconnection", b["interconnection"], f"{b['interconnection']} Interconnection"
            ),
        )
    for sub_id, ba_code in substations:
        s = g.add_entity("Substation", sub_id, sub_id)
        g.set(s, "code", sub_id)
        g.relate(s, "belongsTo", g.entity("BalancingAuthority", ba_code))

    # Derived facts from gold data products
    totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for r in fuel_mix:
        totals[r["ba_code"]][r["fuel_type"]] += max(r["generation_mwh"], 0.0)
    for ba_code, by_fuel in totals.items():
        ba, total = g.entity("BalancingAuthority", ba_code), sum(by_fuel.values()) or 1.0
        shares = {
            f: round(100 * v / total, 1) for f, v in sorted(by_fuel.items(), key=lambda kv: -kv[1])
        }
        g.set(ba, "generationSharePct", json.dumps(shares))
        for fuel, share in shares.items():
            if share >= 5:
                g.relate(ba, "generatesFrom", g.entity("EnergySource", fuel))
    agg: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for r in daily_energy:
        agg[r["ba_code"]].append((r["renewable_share_pct"] or 0.0, r["peak_demand_mwh"]))
    for ba_code, vals in agg.items():
        ba = g.entity("BalancingAuthority", ba_code)
        g.set(ba, "avgRenewableSharePct", round(sum(v[0] for v in vals) / len(vals), 2))
        g.set(ba, "avgDailyPeakMWh", round(sum(v[1] for v in vals) / len(vals), 0))

    # Data estate: datasets, columns, glossary, owners, lineage
    about = {
        "ba_code": "BalancingAuthority",
        "fuel_type": "EnergySource",
        "substation_id": "Substation",
    }
    for c in contracts:
        kind = "DataProduct" if c.layer == "gold" else "Dataset"
        ds = g.add_entity(kind, c.name, c.name, c.description)
        g.set(ds, "classification", c.classification)
        g.set(ds, "layer", c.layer)
        g.set(ds, "contractVersion", c.version)
        g.relate(ds, "ownedBy", g.add_entity("Team", c.owner, c.owner))
        for consumer in c.consumers:
            g.relate(ds, "consumedBy", g.add_entity("Consumer", consumer, consumer))
        for col in c.columns:
            col_iri = g.add_entity(
                "Column", f"{c.name}.{col.name}", f"{c.name}.{col.name}", col.description or None
            )
            g.set(col_iri, "dataType", col.type)
            g.relate(ds, "hasColumn", col_iri)
            if col.business_term:
                g.relate(
                    col_iri,
                    "measures",
                    g.add_entity("BusinessTerm", col.business_term, col.business_term),
                )
            if col.name in about:
                g.relate(ds, "aboutEntityType", ontology.iri(about[col.name]))
    names = {c.name: ("DataProduct" if c.layer == "gold" else "Dataset") for c in contracts}
    for src, _job, dst in lineage_edges:
        if src in names and dst in names and src != dst:
            g.relate(g.entity(names[dst], dst), "derivedFrom", g.entity(names[src], src))
    return g
