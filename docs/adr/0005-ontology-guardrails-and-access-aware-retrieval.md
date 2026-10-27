# ADR 0005: Ontology-validated graph and access-aware retrieval for agents

**Status:** accepted

## Context
Agents grounded on enterprise data fail in two ways: they reason over relationships that
do not exist, and they leak data a user is not cleared to see. Post-filtering an LLM
answer does not fix either.

## Decision
* Every triple is validated against the ontology's classes, domains, ranges and value lists
  at build time. An invalid triple fails the build.
* Each retrievable document carries its source table's classification. The index filters by
  the caller's clearance before scoring, which mirrors Lake Formation for tables and Bedrock
  Knowledge Base metadata filters for documents. Catalog documents are never more open than
  `internal`.
* The graph models the data estate (datasets, columns, business terms, owners, lineage) as
  well as the grid, so an agent can answer "which governed table backs this number?".

## Consequences
* The local implementation has no triple store or vector database dependency. On AWS the
  N-Triples export loads into Amazon Neptune and the documents into a Bedrock Knowledge Base.
* Hashed TF-IDF is a deterministic offline stand-in. `BedrockEmbedder` swaps in Titan
  embeddings without changing the index or the filters.

## Addendum: agents and graph facts (grid analyst agent)

* Graph facts follow the same rule as catalog documents. A dataset or column node, and any
  fact that touches one, is visible only at its metadata classification (never more open than
  `internal`). Substations are known only from confidential telemetry, so they are visible only
  at `confidential`. Grid entities (balancing authorities, regions, energy sources) stay open.
* The agent's clearance is bound when its toolbox is built, never passed by the model. Each
  tool enforces it, so a prompt-injected or confused model can at most ask for data it is
  then refused. On AWS the tools' role has its own Lake Formation ceiling as a second layer.
* Bedrock Knowledge Base retrieval goes through the tools with a `classification` metadata
  filter rather than a knowledge base associated with the Bedrock Agent, because the
  associated path has no per-caller filter.
