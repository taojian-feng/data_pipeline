# ADR 0008: Self-service data products are drafted by an agent and merged as reviewed code

**Status:** accepted

## Context
Analysts and reporting teams keep asking for small variations of the same data products: weekly
instead of daily, per region instead of per balancing authority, renewable share for one board
pack. Each one used to mean a Python change in `transform/gold.py`, a step in
`Pipeline.build_gold`, a contract and tests, so requests queued behind the platform team, and
some teams built their own unmanaged extracts instead. Letting people write SQL straight against
the lake would be faster, but it loses everything ADR 0002 gives us: an owner, a classification,
quality gates, lineage and Lake Formation tags generated from a contract.

## Decision
* A gold data product can be **declared** instead of coded: a contract with a `transform` block
  (`sql`, `inputs`, `grain`) and one SQL file in `transforms/`. The pipeline builds declared
  products after the built-in gold tables, in dependency order, for the periods whose input
  dates changed, through the same strict quality gate and dynamic partition overwrite. Glue
  tables, LF-tags, lineage, the knowledge graph and the RAG catalog pick them up from the
  contract like any other table; no infrastructure changes.
* `check_product` holds the governance rules, and the pipeline enforces them on every build, not
  only at review time: gold only; inputs are other silver or gold tables (never bronze); the SQL
  reads only the inputs it declares (checked with DuckDB's parser, and it runs with file access
  off); the product is classified **at least as strictly as its most restricted input**; it has a
  team owner, consumers, column descriptions, a period date as its partition and first key, and
  an error-severity `unique` rule on its primary key.
* The **self-service agent** (`gridlake propose`) turns a described dataset into such a product. It
  explores with the analyst's read-only tools, previews SQL against today's tables, and drafts the
  contract, the SQL and tests as one patch. The tool, not the model, fixes what must not be
  chosen freely: the inputs come from the SQL, the owner from the request, the classification
  floor from the inputs, and the caller's clearance limits what it can read or build on. A draft
  is saved only if it meets the governance rules, conforms to its contract, passes its quality
  rules and at least one reconciliation check on today's data.
* As in ADR 0006, the agent drafts and a person approves. `gridlake products approve --pr`
  commits the three files on a branch in a temporary worktree, pushes it and opens a pull
  request. CI then runs the drafted tests, which build the product end to end on synthetic data
  and check governance, quality, the reconciliation checks and that a rebuild changes nothing.

## Consequences
* A new product is a reviewed pull request of three small files instead of a Python change; the
  platform team reviews intent and classification rather than writing boilerplate.
* Declared products recompute from their full inputs and keep only the affected periods. That is
  simple and correct for roll-ups of gold tables; a product over large silver history would want
  predicate pushdown into the SQL, which is the next step if one appears.
* The SQL dialect is DuckDB, as for every other transformation (the pipeline runs DuckDB on Lambda
  and Glue). A product meant for Athena consumers is read through the Glue table, not re-run.
* The offline designer covers period roll-ups of energy, renewable share and frequency
  excursions per BA, region or interconnection; other requests need `--llm bedrock`.
