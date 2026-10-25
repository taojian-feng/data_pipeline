# gridlake: an AI-ready energy grid lakehouse

[![ci](https://github.com/taojian-feng/data_pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/taojian-feng/data_pipeline/actions/workflows/ci.yml)
![python](https://img.shields.io/badge/python-3.11%20|%203.12%20|%203.13-blue)
![iceberg](https://img.shields.io/badge/table%20format-Apache%20Iceberg-informational)
![terraform](https://img.shields.io/badge/IaC-Terraform-7B42BC)

A production-grade data pipeline that turns raw US power-grid data (EIA-930 hourly operations
and streaming substation telemetry) into governed data products for BI, ML and AI agents.
It covers ingestion, an Iceberg medallion lakehouse, contract-driven quality gates, OpenLineage
lineage, and a knowledge layer (ontology, knowledge graph, access-aware RAG retrieval).

**It runs end to end on a laptop in about 10 seconds with no cloud account**, and the same code
deploys to AWS (S3 Tables / Glue Data Catalog, Lake Formation, Kinesis, Lambda, Glue Spark,
Step Functions, Bedrock Knowledge Bases) through the Terraform in [`infra/terraform`](infra/terraform).

```bash
make install && make demo        # or: pip install -e ".[dev]" && gridlake run
```

---

## What it demonstrates

| Capability | Where |
|---|---|
| **Lakehouse on Apache Iceberg**: medallion layers, MERGE upserts, dynamic partition overwrite, snapshots, partition evolution | [`catalog.py`](src/gridlake/catalog.py), [ADR 0001](docs/adr/0001-iceberg-everywhere.md) |
| **Same code, three catalogs**: local SQL catalog, AWS Glue Data Catalog, Amazon S3 Tables (Iceberg REST, SigV4) | [`catalog.py`](src/gridlake/catalog.py) |
| **Batch ingestion**: paginated, retrying EIA API v2 client; content-addressed idempotent landing | [`sources/eia.py`](src/gridlake/sources/eia.py), [`ingest/batch.py`](src/gridlake/ingest/batch.py) |
| **Streaming**: at-least-once consumer, commit-after-write checkpoints, per-partition event-time watermarks, exactly-once results | [`ingest/stream.py`](src/gridlake/ingest/stream.py), [`aws_lambda.py`](src/gridlake/aws_lambda.py), [ADR 0004](docs/adr/0004-at-least-once-exactly-once.md) |
| **Incremental processing**: high-water marks, restatement-safe recompute of touched keys, affected-date gold rebuilds | [`pipeline.py`](src/gridlake/pipeline.py), [`transform/`](src/gridlake/transform) |
| **Data contracts** as the control plane for schema, ownership, classification, quality, catalog and access | [`contracts/`](contracts), [ADR 0002](docs/adr/0002-contracts-as-control-plane.md) |
| **Data quality**: declarative rules compiled to SQL, quarantine in silver, circuit breaker in gold, results history | [`quality/engine.py`](src/gridlake/quality/engine.py), [ADR 0003](docs/adr/0003-quarantine-vs-circuit-breaker.md) |
| **Lineage**: OpenLineage START/COMPLETE/FAIL events with schema, ownership, version and DQ facets | [`lineage.py`](src/gridlake/lineage.py) |
| **Data products for multiple consumers**: BI, sustainability reporting, leakage-free ML features, reliability engineering | [`transform/gold.py`](src/gridlake/transform/gold.py) |
| **Ontology + knowledge graph** joining grid topology with the data estate (datasets, columns, glossary, owners, lineage); OWL / Turtle / N-Triples for Neptune | [`knowledge/ontology.yaml`](src/gridlake/knowledge/ontology.yaml), [`knowledge/graph.py`](src/gridlake/knowledge/graph.py) |
| **RAG for agents**: clearance-filtered retrieval, metadata filters, GraphRAG context packs, Bedrock KB export | [`knowledge/retrieval.py`](src/gridlake/knowledge/retrieval.py), [`knowledge/context.py`](src/gridlake/knowledge/context.py), [ADR 0005](docs/adr/0005-ontology-guardrails-and-access-aware-retrieval.md) |
| **Least-privilege governance**: Lake Formation tag-based access control generated from contracts, KMS, TLS-only buckets | [`governance.tf`](infra/terraform/governance.tf), [`iam.tf`](infra/terraform/iam.tf) |
| **Operations**: Step Functions with retries and alerts, DLQ, iterator-age alarms, nightly Iceberg maintenance, runbook | [`orchestration.tf`](infra/terraform/orchestration.tf), [`monitoring.tf`](infra/terraform/monitoring.tf), [runbook](docs/runbook.md) |
| **Validating AI-assisted code** in a regulated setting | [docs/ai-assisted-delivery.md](docs/ai-assisted-delivery.md) |

## Architecture

```mermaid
flowchart LR
  EIA[EIA-930 API] -->|idempotent batches| BR[(Bronze<br/>as received)]
  DEV[Substation telemetry] -->|at-least-once stream| BR
  BR -->|type, dedupe, conform<br/>quality gate + quarantine| SV[(Silver)]
  SV -->|data products<br/>strict quality gate| GD[(Gold)]
  GD --> BI[BI / reporting]
  GD --> ML[SageMaker features]
  GD --> KL[Knowledge layer<br/>ontology, graph, RAG index]
  KL --> AG[AI agents / Bedrock]
  C[[Data contracts]] -.schema, rules, access.-> BR & SV & GD
  C -.-> TF[Terraform: Glue tables,<br/>Lake Formation LF-tags]
  OL[(OpenLineage)] -.-> KL
```

Details, the AWS deployment diagram and the local-vs-AWS mapping are in
[docs/architecture.md](docs/architecture.md).

### Tables

| Layer | Table | Grain | Classification |
|---|---|---|---|
| bronze | `eia_region_data`, `eia_fuel_type_data` | API record + batch metadata | internal |
| bronze | `grid_telemetry` | stream event (with duplicates) | confidential |
| silver | `dim_balancing_authority`, `dim_fuel_type` | entity | public |
| silver | `demand_hourly` | BA x hour | internal |
| silver | `generation_hourly` | BA x hour x energy source | internal |
| silver | `grid_telemetry_minute` | substation x minute (exactly-once) | confidential |
| gold | `ba_daily_energy` | BA x day: demand, peak, forecast MAPE, renewable share, carbon intensity | public |
| gold | `fuel_mix_daily` | BA x day x energy source | public |
| gold | `demand_forecast_features` | BA x hour, point-in-time-correct lags | internal |
| gold | `grid_reliability_daily` | BA x day frequency excursions | confidential |
| ops | `quality.results`, `quality.quarantine` | rule evaluation / rejected row | internal / confidential |

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

gridlake run --days 14            # full pipeline into ./.gridlake (Iceberg warehouse + catalog)
gridlake quality                  # rule outcomes and quarantined rows
gridlake lineage                  # dataset lineage from OpenLineage events
gridlake sql "SELECT * FROM gold_ba_daily_energy ORDER BY date DESC LIMIT 5"
gridlake ask "What was the renewable share for CAISO on 2026-10-20?"
pytest                            # 40+ tests including end-to-end idempotency and failure paths
```

Real EIA data: get a free key at <https://www.eia.gov/opendata/register.php> and
`export EIA_API_KEY=...`. Without a key the pipeline uses a deterministic synthetic replica of
EIA-930 that reproduces real feed defects (missing hours, malformed values, sign errors,
unregistered respondents, restatements, stream duplicates and late events).

Docker: `docker build -t gridlake . && docker run --rm -v gridlake:/data gridlake run --days 14`

### Sample output (synthetic data, 14 days, 8 balancing authorities)

```text
COMPLETE  seed_dimensions                 0.24s  {"dim_balancing_authority": {"rows": 8}, "dim_fuel_type": {"rows": 8}}
COMPLETE  ingest_eia                      1.03s  {"bronze.eia_region_data": {"rows": 10753}, "bronze.eia_fuel_type_data": {"rows": 16128}}
COMPLETE  ingest_stream                   1.15s  {"produced": 35241, "consumed": 35241, "micro_batches": 9, "late_events": 123}
COMPLETE  silver_demand_hourly            0.50s  {"rows": 2670, "quarantined": 19}
COMPLETE  silver_generation_hourly        3.85s  {"rows": 16009, "quarantined": 119}
COMPLETE  silver_grid_telemetry_minute    1.16s  {"rows": 8640, "quarantined": 0}
COMPLETE  gold_ba_daily_energy            0.17s  {"rows": 112, "quarantined": 0}
COMPLETE  gold_fuel_mix_daily             0.18s  {"rows": 672, "quarantined": 0}
COMPLETE  gold_demand_forecast_features   0.18s  {"rows": 2670, "warnings": ["not_null(demand_lag_24h)"]}
COMPLETE  gold_grid_reliability_daily     0.11s  {"rows": 8, "quarantined": 0}
COMPLETE  knowledge_layer                 0.16s  {"graph": {"triples": 977, ...}, "documents": 138}
```

```text
$ gridlake sql "SELECT ba_code, round(avg(renewable_share_pct),1) renewable_pct, round(avg(carbon_intensity_kg_per_mwh)) kg_co2_per_mwh FROM gold_ba_daily_energy GROUP BY 1 ORDER BY 2 DESC"
 ba_code  renewable_pct  kg_co2_per_mwh
 BPAT              90.6             9.0
 CISO              34.6           219.0
 SWPP              29.2           471.0
 NYIS              26.9           214.0
 ERCO              20.3           374.0
 ISNE              13.2           256.0
 MISO              11.4           528.0
 PJM                5.6           358.0

$ gridlake sql "SELECT failed_rules, count(*) n FROM quality_quarantine GROUP BY 1 ORDER BY 2 DESC"
 not_null(period,ba_code,fuel_type,generation_mwh,_updated_ts)   119
 not_null(demand_mwh)                                             15
 range(demand_mwh:0..250000)                                       3
 referential(ba_code->silver.dim_balancing_authority.ba_code)      1
```

A grounded context pack for an agent: entities are linked through the knowledge graph and
passages are filtered by the caller's clearance before ranking.

```text
$ gridlake ask "Which balancing authority had the highest renewable share, and which table should I use?" --clearance internal
## Knowledge graph facts
- Renewable Share is a BusinessTerm
- gold.ba_daily_energy.renewable_share_pct measures Renewable Share
## Retrieved passages
[ranking/renewable_share_pct] Ranking of balancing authorities by average renewable share (highest first),
  2026-10-10 to 2026-10-23: 1. Bonneville Power Administration (BPAT) 90.6%; 2. California Independent System Operator (CISO) 34.6%; ...
[catalog/gold.ba_daily_energy] Table gold.ba_daily_energy is a gold-layer dataset ... owned by grid-analytics@gridlake.example, classified public ...
```

The same question with `--clearance public` never sees confidential telemetry or internal catalog
entries, which is the behaviour Lake Formation enforces for SQL users.

## Guarantees backed by tests

| Guarantee | Test |
|---|---|
| Re-running the same window changes nothing (no new snapshots, no duplicate rows) | `test_rerun_is_idempotent` |
| Replayed stream events do not double count | `test_redelivered_stream_events_do_not_double_count` |
| An uncommitted micro-batch is replayed after a crash | `test_uncommitted_batch_is_replayed_after_crash` |
| EIA restatements flow through to gold without touching other partitions | `test_restatement_flows_through_to_gold` |
| A gold quality failure publishes nothing and emits a lineage FAIL event | `test_quality_gate_stops_publication` |
| ML features use only information available 24h before the target | `test_forecast_features_are_point_in_time_correct` |
| Step-by-step orchestration (as in Step Functions) equals a single run | `test_step_mode_matches_full_run` |
| Invalid relationships are rejected by the ontology | `test_ontology_guardrails_reject_invalid_triples` |
| Retrieval never returns documents above the caller's clearance | `test_retrieval_enforces_clearance_before_ranking` |

## Deploying to AWS

```bash
cd infra/terraform
terraform init -backend-config="bucket=<state-bucket>" -backend-config="key=gridlake/dev.tfstate" -backend-config="region=us-east-1"
terraform apply -var environment=dev -var 'analyst_principal_arns=["arn:aws:iam::<acct>:role/Analyst"]'
```

What gets created: KMS-encrypted, TLS-only S3 buckets (landing, warehouse, artifacts, knowledge);
an S3 Tables bucket for gold products; Glue databases and one Iceberg table per contract; Lake
Formation LF-tags (`classification` x `layer`) and tag-based grants; a Kinesis stream with a
Lambda consumer (bisect on error, DLQ); pipeline Lambdas from one container image; a Glue 5.0
Spark job for MERGE backfills and a nightly Iceberg maintenance job; a Step Functions state
machine on an EventBridge schedule that ends with a Bedrock Knowledge Base sync; SNS alerts and
CloudWatch alarms. Point a Bedrock Knowledge Base at the `knowledge_base_source` output and pass
its ids back in as variables to enable the sync step.

Runtime switches: `GRIDLAKE_CATALOG=glue` with `GRIDLAKE_WAREHOUSE=s3://...`, or
`GRIDLAKE_CATALOG=s3tables` with `GRIDLAKE_S3_TABLES_ARN=arn:aws:s3tables:...`.

## Repository layout

```text
contracts/            data contracts (one YAML per table): schema, PK, partitioning, owner, classification, rules
seeds/                reference data (balancing authorities, energy sources)
src/gridlake/
  sources/            EIA API client, synthetic EIA-930 replica, telemetry generator
  ingest/             idempotent batch landing, streaming consumer (log, offsets, watermarks)
  transform/          silver and gold SQL (DuckDB over Arrow)
  quality/            contract-driven quality engine
  knowledge/          ontology, knowledge graph, retrieval index, GraphRAG context packs
  catalog.py          Iceberg catalogs (local / Glue / S3 Tables) and contract-bound tables
  pipeline.py         orchestration, lineage, quality gates, run manifests
  aws_lambda.py       Kinesis consumer and Step Functions task handlers
spark/                Glue 5.0 Spark jobs: Iceberg MERGE backfill, table maintenance
infra/terraform/      AWS infrastructure
docs/                 architecture, ADRs, runbook, AI-assisted delivery
tests/                unit and end-to-end tests
```

## Scope and limits

* Synthetic data is the default so the project runs anywhere; emission factors are illustrative.
* The local engine is single-process; the Glue Spark job is the scale-out path for backfills.
* The hashed TF-IDF embedder is a deterministic offline stand-in for Titan embeddings
  (`BedrockEmbedder`), and the in-memory graph is a stand-in for Amazon Neptune.
* The Terraform passes `terraform validate` in CI; applying it needs your own AWS account and
  an image pushed to the ECR repository it creates.

## License

MIT
