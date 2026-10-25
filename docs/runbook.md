# Runbook

## Backfill a date range
```bash
gridlake run --start 2026-01-01 --end 2026-02-01 --no-telemetry
```
On AWS, start the state machine with `{"backfill": true}` to route silver through the Glue
Spark job. Bronze landing is idempotent, so overlapping windows are safe.

## Triage quarantined rows
```bash
gridlake quality --samples 20
gridlake sql "SELECT failed_rules, count(*) FROM quality_quarantine GROUP BY 1 ORDER BY 2 DESC"
```
Fix the cause upstream, or fix the reference data (for example add a new BA to
`seeds/balancing_authorities.csv`), then re-run. Silver recomputes the touched keys.

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
