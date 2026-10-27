# gridlake: an AI-ready energy grid lakehouse

[![ci](https://github.com/taojian-feng/data_pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/taojian-feng/data_pipeline/actions/workflows/ci.yml)
![python](https://img.shields.io/badge/python-3.11%20|%203.12%20|%203.13-blue)
![iceberg](https://img.shields.io/badge/table%20format-Apache%20Iceberg-informational)
![terraform](https://img.shields.io/badge/IaC-Terraform-7B42BC)

A production-grade data pipeline that turns raw US power-grid data (EIA-930 hourly operations
and streaming substation telemetry) into governed data products for BI, ML and AI agents.
It covers ingestion, an Iceberg medallion lakehouse, contract-driven quality gates, OpenLineage
lineage, a knowledge layer (ontology, knowledge graph, access-aware RAG retrieval), a
**grid analyst agent** that answers questions by choosing its own tools and cites the tables it used,
and a **data reliability agent** that triages failed runs and quality spikes and drafts a fix or
GitHub issue for a person to approve.

**It runs end to end on a laptop in about 10 seconds with no cloud account**, and the same code
deploys to AWS (S3 Tables / Glue Data Catalog, Lake Formation, Kinesis, Lambda, Glue Spark,
Step Functions, Bedrock Knowledge Bases, Bedrock Agents, Athena) through the Terraform in [`infra/terraform`](infra/terraform).

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
| **Grid analyst agent**: the model picks tools (SQL on gold, knowledge graph, document search, catalog) and cites tables; caller clearance is bound outside the model and enforced in every tool, then again by Lake Formation; runs offline or on Claude via Amazon Bedrock, or as a Bedrock Agent | [`agent/`](src/gridlake/agent), [`agent.tf`](infra/terraform/agent.tf), [ADR 0005](docs/adr/0005-ontology-guardrails-and-access-aware-retrieval.md) |
| **Data reliability agent**: when a run fails or a quality rule spikes, it reads the run log, quality history, rejected rows, lineage and contract, records a diagnosis with evidence, and drafts a contract/seed patch or a GitHub issue. Drafts change nothing until a person approves them; fixes are confined to contracts and seeds, must validate, and never loosen a gold rule | [`reliability/`](src/gridlake/reliability), [`reliability.tf`](infra/terraform/reliability.tf), [ADR 0006](docs/adr/0006-agents-draft-people-approve.md) |
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
gridlake ask "What was the renewable share for CAISO on 2026-10-20?" --trace   # grid analyst agent
gridlake run --inject-fault new_fuel_type && gridlake triage --trace           # data reliability agent
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

### Grid analyst agent

`gridlake ask` runs a tool-using agent. The model decides which tools to call: `lookup_entity`
resolves "California ISO" to `CISO` through the knowledge graph, `run_sql` queries the gold
tables, `describe_table` names the owner and lineage, and `search_documents` retrieves passages.
Every answer lists the tables and documents it came from.

```text
$ gridlake ask "Which balancing authority had the highest renewable share, and which table should I use?" --clearance internal --trace
> lookup_entity({"text": "Which balancing authority had the highest renewable share, and which table should I use?"}) -> ok
> run_sql({"sql": "SELECT ba_code, min(date) AS first_day, max(date) AS last_day, round(avg(renewable_share_pct), 2) AS value FROM gold.ba_daily_energy GROUP BY ba_code ORDER BY value DESC LIMIT 10"}) -> ok
> describe_table({"table": "gold.ba_daily_energy"}) -> ok

BPAT ranks first by average renewable share (90.6%, 2026-10-10 to 2026-10-23). Ranking: 1. BPAT 90.6%; 2. CISO 34.6%; ...
The governed table is gold.ba_daily_energy (public, owned by grid-analytics@gridlake.example, contract v1.0.0),
derived from silver.demand_hourly, silver.dim_balancing_authority, silver.dim_fuel_type, silver.generation_hourly, ...

sources: tables=gold.ba_daily_energy; documents=knowledge-graph  [offline-planner, clearance=internal]

$ gridlake ask "Which balancing authority had the most frequency excursion minutes?" --trace
> lookup_entity(...) -> ok
> run_sql({"sql": "SELECT ba_code, ... FROM gold.grid_reliability_daily GROUP BY ba_code ..."}) -> error: access denied:
  gold.grid_reliability_daily is classified confidential; caller clearance is public
```

**Access is enforced outside the model.** The caller's clearance is bound when the toolbox is
built (from the CLI flag locally, from Bedrock Agent session attributes set by the calling app on
AWS). The model never sees a clearance parameter and cannot pass one. Each tool checks it:

| Tool | Enforcement |
|---|---|
| `run_sql` | DuckDB's parser lists every table the statement reads (through CTEs, joins and subqueries). Anything but one `SELECT`, any table function (`read_csv`, `read_parquet`) and any table above clearance is refused before the query runs. Locally only the checked tables are attached and file access is switched off; on Athena, Lake Formation grants the tools' role tables up to `analyst_agent_max_clearance` only. |
| `list_tables` | Lists only tables the caller may query. |
| `describe_table` | Catalog metadata (owner, lineage, rules) is never more open than `internal`. |
| `lookup_entity` | Graph facts touching a dataset, column or substation above clearance are dropped. |
| `search_documents` | Local index filters by clearance before ranking. On AWS, Bedrock `Retrieve` gets a `classification` metadata filter, and returned hits are checked again. |

**Models.** Without a cloud account the agent runs on `OfflinePlanner`, a deterministic,
rule-based stand-in (not an LLM) that covers metric lookups, rankings and catalog questions.
With AWS credentials, `pip install -e ".[agent]"` and `gridlake ask "..." --llm bedrock` runs the
same loop on Claude in Amazon Bedrock (`GRIDLAKE_AGENT_MODEL`, default `anthropic.claude-opus-5-5`).
Set `GRIDLAKE_AGENT_SQL=athena` and `GRIDLAKE_ATHENA_WORKGROUP` to query Athena instead of the local
lake, and `GRIDLAKE_BEDROCK_KB_ID` to search the Bedrock Knowledge Base.

`gridlake ask --context-only` still prints the GraphRAG context pack (linked entities, graph
facts and clearance-filtered passages) for use in another model's prompt.

### Data reliability agent

`gridlake triage` looks at a run (the latest by default). Every failed step is an incident, and so
is any error rule that rejected at least 2% (and 5) of a table's rows: the synthetic feed's
background defect rate stays below that. For each incident the agent investigates with read-only
tools, records a diagnosis (category, cause, evidence, confidence, owner) and drafts one
remediation. `gridlake run --inject-fault` reproduces three realistic failures:

| Fault | What the agent finds | Draft |
|---|---|---|
| `new_fuel_type`: EIA starts reporting battery storage (`BAT`) | `reference_data_gap`: valid rows rejected by the `fuel_type` referential rule; `BAT` is an EIA code missing from `seeds/fuel_types.csv` | **fix**: a seed patch, checked against the dimension's own rules |
| `unit_change`: ERCO demand arrives in kWh | `source_data_defect`: rejected ERCO demand is x1,030 its accepted history; the rule is right | **issue** to the bronze source owner, with the downstream impact |
| `feed_outage`: the EIA API returns 503 until retries run out | `upstream_outage`: ingest failed, nothing later ran; landing is idempotent | **issue** with the safe re-run |

```text
$ gridlake run --inject-fault new_fuel_type && gridlake triage --trace
== inc-35876a0a-1: silver.generation_hourly: referential(fuel_type->silver.dim_fuel_type.fuel_type) rejected 672 of 16800 rows (4.0%)
> get_run_log({}) -> ok
> get_quality_results({"table": "silver.generation_hourly"}) -> ok
> get_lineage({"table": "silver.generation_hourly"}) -> ok
> get_contract({"table": "silver.generation_hourly"}) -> ok
> inspect_quarantine({"table": "silver.generation_hourly", "rule": "referential(fuel_type->...)", "limit": 3}) -> ok
> get_contract({"table": "silver.dim_fuel_type"}) -> ok
> read_repo_file({"path": "seeds/fuel_types.csv"}) -> ok
> record_diagnosis({"category": "reference_data_gap", ...}) -> ok
> draft_fix({"path": "seeds/fuel_types.csv", "edits": [...]}) -> ok
reference_data_gap (high confidence): EIA now reports energy source BAT, which silver.dim_fuel_type does not
list yet, so those rows are quarantined. The data is valid; the reference seed is behind.
  draft inc-35876a0a-1-d1 [fix] Register EIA energy source(s) BAT in seeds/fuel_types.csv

$ gridlake incidents show inc-35876a0a-1-d1        # the rationale, evidence and the diff
$ gridlake incidents approve inc-35876a0a-1-d1 --apply   # a person decides; git apply to the working tree
$ gridlake run && gridlake replay silver.generation_hourly   # reload the seed, publish the rejected rows
COMPLETE  replay_generation_hourly       {"replayed": 781, "rows": 672, "still_failing": 109}
COMPLETE  gold_fuel_mix_daily            {"rows": 700, "quarantined": 0}
```

**The agent drafts, a person approves** ([ADR 0006](docs/adr/0006-agents-draft-people-approve.md)).
Drafts are pending files (`.gridlake/incidents/`, or S3 on AWS). `gridlake incidents approve`
files an issue through the GitHub API when `GITHUB_TOKEN` and `GRIDLAKE_GITHUB_REPO` are set
(else exports Markdown for `gh issue create`), or writes a fix as a patch (`--apply` runs
`git apply` for the usual pull-request review). `reject --reason` records the decision.

| Guardrail | Where |
|---|---|
| Fixes may only touch `contracts/*.yaml` and `seeds/*.csv` | `draft_fix` refuses any other path |
| A changed contract must still parse and bump its version; gold quality rules are never changed | `_validate_contract` |
| A seed change must pass its dimension's own contract rules (unique keys, ranges) | `_validate_seed` |
| Nothing is drafted before a diagnosis with evidence is recorded; at most three drafts per incident | `record_diagnosis`, `MAX_DRAFTS` |
| Rejected rows are as sensitive as the table they were meant for | `inspect_quarantine` checks the caller's clearance against that table |

It runs on the same tool loop as the grid analyst (`gridlake.agent.core.run_loop`) with the
deterministic `OfflineResponder` by default, or Claude with `--llm bedrock`. On AWS
(`-var enable_reliability_agent=true`) an EventBridge rule sends every finished Step Functions
execution to a Lambda that reads the execution history, scopes quality results to the
execution's time window, writes drafts to `s3://<artifacts>/incidents/` and posts a summary to
the alerts topic.

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
| Graph facts about datasets above the caller's clearance stay out of context packs | `test_context_pack_hides_data_estate_facts_above_clearance` |
| The agent's SQL tool refuses writes, file reads and tables above clearance | `test_only_single_selects_over_governed_tables`, `test_run_sql_enforces_clearance_before_querying` |
| The model cannot raise its own clearance; Bedrock Agents take it from the session | `test_model_cannot_pass_its_own_clearance`, `test_action_group_takes_clearance_from_the_session` |
| Bedrock KB retrieval is filtered by classification server-side and checked again | `test_bedrock_retrieve_filters_by_classification_server_and_client_side` |
| The reliability agent tells a source defect from a stale seed, and an outage from both | `test_unit_change_is_a_source_defect_and_never_a_contract_change`, `test_new_fuel_type_fix_applies_and_replay_restores_the_rows`, `test_feed_outage_is_an_upstream_outage` |
| Agent fixes stay in contracts and seeds, bump versions, never loosen gold, and pass the dimension's rules | `test_fixes_are_confined_to_contracts_and_seeds`, `test_contract_fixes_must_bump_the_version_and_never_loosen_gold`, `test_seed_fixes_must_pass_the_dimension_rules` |
| Nothing is filed or applied until a person approves | `test_new_fuel_type_fix_applies_and_replay_restores_the_rows`, `test_approving_an_issue_files_it_on_github` |

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

Grid analyst agent (off by default): `-var enable_analyst_agent=true` adds an Athena workgroup with
a 1 GiB scan cutoff, a tools Lambda (same image, `gridlake.agent.bedrock_action_group.handler`) with
a Lake Formation grant capped at `analyst_agent_max_clearance`, and a Bedrock Agent whose
instructions and action-group functions come from the repo (`src/gridlake/agent/instructions.txt`,
`infra/terraform/agent_functions.json`, kept in sync with the tools by a test). The calling app
passes the user's clearance as `sessionState.sessionAttributes.clearance` on `InvokeAgent`. Set
`analyst_agent_model` to a model or inference profile enabled for Bedrock Agents in your account.

Data reliability agent (off by default): `-var enable_reliability_agent=true` adds the
EventBridge rule, the Lambda (`gridlake.reliability.aws.handler`), a read-only role (execution
history, quality tables, lake reads up to `reliability_agent_clearance`, drafts under
`incidents/`, publish to the alerts topic) and the `reliability_incidents_uri` output. It runs the
offline responder unless `reliability_agent_llm=bedrock`. Review drafts from a laptop with
`GRIDLAKE_INCIDENTS_S3_URI=<that uri> gridlake incidents list`.

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
  agent/              grid analyst: agent loop, access-aware tools, Bedrock / offline models, Bedrock Agents action group
  reliability/        data reliability agent: incident detection, triage tools, drafts and approval, fault injection
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
* The offline planner and responder are scripted stand-ins so the agents run anywhere; open-ended
  questions and incidents outside their decision tables need `--llm bedrock`. The Bedrock and Athena paths are covered by tests with stubbed AWS clients.
* The Terraform passes `terraform validate` in CI; applying it needs your own AWS account and
  an image pushed to the ECR repository it creates.

## License

MIT
