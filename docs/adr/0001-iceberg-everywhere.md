# ADR 0001: Apache Iceberg locally and in AWS

**Status:** accepted

## Context
A portfolio pipeline has to run on a laptop and in CI with no cloud credentials, yet the
code must be the code you would deploy. Parquet-on-disk locally plus Iceberg in AWS
would mean two write paths and untested production semantics (MERGE, snapshots,
partition overwrite, schema enforcement).

## Decision
Use Iceberg v2 through `pyiceberg` everywhere. Locally the catalog is the Iceberg SQL
catalog on SQLite; on AWS it is the Glue Data Catalog or Amazon S3 Tables through its
Iceberg REST endpoint. DuckDB does the in-process SQL over Arrow scans.

## Consequences
* Upserts, dynamic partition overwrite, snapshot ids for lineage, and partition evolution
  are exercised by the test suite.
* Local scale is bounded by a single process. Backfills at scale use the Glue Spark
  job (`spark/glue_silver_demand_hourly.py`), which keeps the same semantics in Spark SQL.
* Amazon S3 Tables removes the need for the maintenance job for tables stored there.
