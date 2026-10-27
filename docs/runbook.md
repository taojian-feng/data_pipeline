# Runbook

## Backfill a date range
```bash
gridlake run --start 2026-01-01 --end 2026-02-01 --no-telemetry
```
On AWS, start the state machine with `{"backfill": true}` to route silver through the Glue
Spark job. Bronze landing is idempotent, so overlapping windows are safe.

## Triage an incident with the data reliability agent
```bash
gridlake triage --trace                 # the latest run; --run <id> for another
gridlake incidents list                 # pending drafts
gridlake incidents show <draft>         # diagnosis, evidence and the issue body or diff
gridlake incidents approve <draft>      # file the issue (GITHUB_TOKEN + GRIDLAKE_GITHUB_REPO) or write the patch
gridlake incidents approve <draft> --apply   # ... and git apply it, then open a pull request
gridlake incidents reject <draft> --reason "..."
```
On AWS the agent runs after every pipeline execution and posts to the alerts topic; set
`GRIDLAKE_INCIDENTS_S3_URI` to the `reliability_incidents_uri` output to review its drafts.

## Triage quarantined rows
```bash
gridlake quality --samples 20
gridlake sql "SELECT failed_rules, count(*) FROM quality_quarantine GROUP BY 1 ORDER BY 2 DESC"
```
If the source sent bad values, ask it to restate: restated hours land as a new batch and silver
recomputes the keys they touch. If the reference data or contract is behind (for example a new BA
or energy source), fix the seed or contract, run the pipeline to reload it, then replay:
```bash
gridlake run && gridlake replay silver.generation_hourly --rule "referential(fuel_type"
```
Re-running the window alone does not bring rejected rows back, because bronze landing skips
batches it has already seen.

## Fold in late telemetry
```bash
gridlake run --no-telemetry --reconcile
```

## A gold quality gate failed
The step raised `QualityGateError`. Nothing was published and the previous snapshot is
still served. Check `quality.results` for the failing rule and the OpenLineage `FAIL` event
in `.gridlake/lineage/` (or your lineage backend). A gold failure is a transformation bug,
so fix and redeploy rather than relaxing the rule.

## Time travel and rollback
Every write is an Iceberg snapshot. The snapshot id of each output is recorded in the
lineage `version` facet. Roll back with `CALL system.rollback_to_snapshot(...)` in
Athena or Spark.

## Add a data product
1. Add `contracts/gold.<name>.yaml` with owner, classification, consumers, primary key and a
   `unique` rule on it (enforced by tests).
2. Add the SQL in `gridlake/transform/gold.py` and a step in `Pipeline.build_gold`.
3. `terraform apply` creates the Glue table and its LF-tags from the contract.
