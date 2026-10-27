import pytest

from conftest import REPO
from gridlake.knowledge.context import build_context
from gridlake.knowledge.graph import KnowledgeGraph, Ontology, OntologyViolation, build_graph
from gridlake.knowledge.retrieval import Document, VectorIndex, export_bedrock_kb
from gridlake.transform import silver


@pytest.fixture
def graph(contracts):
    edges = [
        ("silver.demand_hourly", "gold_ba_daily_energy", "gold.ba_daily_energy"),
        ("bronze.eia_region_data", "silver_demand_hourly", "silver.demand_hourly"),
    ]
    fuel_mix = [
        {"ba_code": "CISO", "fuel_type": "SUN", "generation_mwh": 30.0},
        {"ba_code": "CISO", "fuel_type": "NG", "generation_mwh": 68.0},
        {"ba_code": "CISO", "fuel_type": "OIL", "generation_mwh": 2.0},
    ]
    return build_graph(
        Ontology.load(),
        silver.dim_balancing_authority(REPO / "seeds").to_pylist(),
        silver.dim_fuel_type(REPO / "seeds").to_pylist(),
        contracts.values(),
        edges,
        fuel_mix=fuel_mix,
    )


def test_ontology_guardrails_reject_invalid_triples():
    g = KnowledgeGraph(Ontology.load())
    ba = g.add_entity("BalancingAuthority", "CISO", "CAISO")
    src = g.add_entity("EnergySource", "SUN", "Solar")
    with pytest.raises(OntologyViolation, match="is not a Region"):
        g.relate(ba, "operatesInRegion", src)
    with pytest.raises(OntologyViolation, match="unknown object property"):
        g.relate(ba, "ownsPlant", src)
    with pytest.raises(OntologyViolation, match="expected boolean"):
        g.set(src, "isRenewable", "yes")
    with pytest.raises(OntologyViolation, match="unknown class"):
        g.add_entity("PowerPlant", "x", "x")


def test_graph_links_grid_and_data_estate(graph):
    ciso = graph.entity("BalancingAuthority", "CISO")
    facts = graph.describe(ciso)
    assert "California Independent System Operator operates in region California" in facts
    generates = {graph.label(o) for o in graph.objects(ciso, "generatesFrom")}
    assert generates == {"Solar", "Natural Gas"}  # OIL is below the 5% materiality threshold
    gold = graph.entity("DataProduct", "gold.ba_daily_energy")
    assert [graph.label(d) for d in graph.upstream(gold)] == [
        "silver.demand_hourly",
        "bronze.eia_region_data",
    ]


def test_exports_are_valid_rdf_shapes(graph):
    nt = graph.to_ntriples().splitlines()
    assert len(nt) == len(graph.triples)
    assert all(line.startswith("<") and line.endswith(" .") for line in nt)
    ttl = Ontology.load().to_turtle()
    assert "gl:BalancingAuthority a owl:Class" in ttl and "rdfs:subClassOf gl:GridEntity" in ttl


def docs():
    return [
        Document(
            "a",
            "CISO renewable share was 40% on 2026-10-01",
            {
                "classification": "public",
                "ba_code": "CISO",
                "doc_type": "fact",
                "date": "2026-10-01",
            },
        ),
        Document(
            "b",
            "ERCO had 12 frequency excursion minutes",
            {"classification": "confidential", "ba_code": "ERCO", "doc_type": "fact"},
        ),
        Document(
            "c",
            "Table gold.ba_daily_energy holds renewable share",
            {"classification": "internal", "doc_type": "catalog"},
        ),
    ]


def test_retrieval_enforces_clearance_before_ranking():
    idx = VectorIndex().build(docs())
    assert [h.document.doc_id for h in idx.search("frequency excursion", clearance="public")] == []
    assert idx.search("frequency excursion", clearance="confidential")[0].document.doc_id == "b"
    assert {h.document.doc_id for h in idx.search("renewable share", clearance="internal")} == {
        "a",
        "c",
    }


def test_index_roundtrip_and_bedrock_export(tmp_path):
    idx = VectorIndex().build(docs())
    idx.save(tmp_path / "idx")
    again = VectorIndex.load(tmp_path / "idx")
    assert (
        again.search("renewable", clearance="internal")[0].document.doc_id
        == idx.search("renewable", clearance="internal")[0].document.doc_id
    )
    assert export_bedrock_kb(docs(), tmp_path / "kb") == 3
    assert len(list((tmp_path / "kb").glob("*.metadata.json"))) == 3


def test_context_pack_links_aliases_and_filters(graph):
    idx = VectorIndex().build(docs())
    pack = build_context(graph, idx, "What was California ISO's renewable share on 2026-10-01?")
    assert "California Independent System Operator" in pack.entities
    assert [p["doc_id"] for p in pack.passages] == ["a"]
    assert "cite passage ids" in pack.to_prompt()


def test_summary_documents_answer_superlatives():
    from gridlake.knowledge.retrieval import summary_documents

    rows = [
        {
            "date": "2026-10-01",
            "ba_code": c,
            "ba_name": c,
            "renewable_share_pct": r,
            "carbon_intensity_kg_per_mwh": ci,
            "forecast_mape_pct": 2.0,
            "peak_demand_mwh": 100.0,
        }
        for c, r, ci in (("BPAT", 90.0, 9.0), ("PJM", 5.0, 360.0))
    ]
    docs = {d.doc_id: d for d in summary_documents(rows, "public")}
    assert "1. BPAT (BPAT) 90.0%; 2. PJM (PJM) 5.0%" in docs["ranking/renewable_share_pct"].text
    idx = VectorIndex().build(list(docs.values()))
    hit = idx.search("which balancing authority had the highest renewable share", k=1)[0]
    assert hit.document.doc_id == "ranking/renewable_share_pct"


def test_context_pack_hides_data_estate_facts_above_clearance(graph):
    idx = VectorIndex().build(docs())
    question = "Which table holds Renewable Share for CAISO?"
    public = build_context(graph, idx, question, clearance="public")
    internal = build_context(graph, idx, question, clearance="internal")
    measures = "gold.ba_daily_energy.renewable_share_pct measures Renewable Share"
    assert measures not in public.graph_facts and measures in internal.graph_facts
    assert "California Independent System Operator" in public.entities  # grid facts stay open


def test_graph_roundtrips_through_ntriples(graph):
    again = KnowledgeGraph.from_ntriples(Ontology.load(), graph.to_ntriples())
    assert again.stats() == graph.stats()
    ciso = graph.entity("BalancingAuthority", "CISO")
    assert again.describe(ciso) == graph.describe(ciso)
    assert again.to_ntriples() == graph.to_ntriples()
