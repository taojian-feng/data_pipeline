# ADR 0002: Data contracts as the single control plane

**Status:** accepted

## Context
Schema, ownership, classification and quality expectations usually live in five places:
DDL, a catalog, IAM/Lake Formation, a DQ tool and a wiki. They drift.

## Decision
One YAML contract per table in `contracts/`. Python reads it for DDL, casting, quality and
lineage. Terraform reads it (`yamldecode`) for Glue tables and Lake Formation LF-tags. The
knowledge layer reads it for catalog documents and the data-estate graph.

## Consequences
* Adding a data product means adding a contract and a transform. Governance, docs and
  agent context come with it.
* Contract changes are code-reviewed and tested (`tests/test_contracts.py` enforces that
  every gold product has an owner, consumers and a primary-key uniqueness rule).
* Breaking schema changes need a version bump and a migration, which is deliberate.
