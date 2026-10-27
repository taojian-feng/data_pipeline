# ADR 0006: The data reliability agent drafts; people approve

**Status:** accepted

## Context
Most pipeline incidents are diagnosed by the same few steps: read the run log, compare the
failing rule with its history, look at the rejected rows, walk the lineage, read the contract.
An agent can do that in seconds. The fixes are another matter. Loosening a quality rule to make
an alert go away, or "fixing" a source defect in our seeds, silently publishes bad data to every
consumer. In a regulated energy setting, a person has to own each change.

## Decision
* The agent's tools are read-only, except two that write **pending drafts**: a GitHub issue, or
  a fix as a unified diff. Approving a draft (`gridlake incidents approve`) is the only path to
  an effect: filing the issue, or writing the patch and optionally `git apply` to a working tree,
  where the normal pull-request review and CI still apply.
* A diagnosis (category, cause, evidence, confidence, owner) must be recorded before anything is
  drafted, so every draft carries the reasoning a reviewer needs.
* Fixes are confined to `contracts/*.yaml` and `seeds/*.csv`, and validated before they are
  offered: a changed contract must parse and bump its version, gold quality rules are never
  changed (a gold gate failure is a transformation bug, per ADR 0003), and a seed change must
  pass its dimension's own contract rules. Everything else becomes an issue for the owner.
* The categories separate "the data is wrong" (source defect: an issue to the source owner, the
  rule stays) from "the contract is behind" (reference gap or out-of-date bound: a fix).
* Access follows ADR 0005: clearance is bound outside the model, and rejected rows are as
  sensitive as the table they were meant for.
* A deterministic `OfflineResponder` runs the same loop without a model, so the behaviour is
  testable and the demo runs anywhere; Claude on Bedrock handles incidents outside its table.

## Consequences
* After an approved seed or contract fix, `gridlake replay <table>` re-checks quarantined rows and
  publishes the ones that now pass. Re-running the window alone would not: bronze landing is
  idempotent, so unchanged batches are skipped.
* Detection uses a failure-rate threshold (2% and 5 rows by default) to stay above the feed's
  background defect rate. Hourly runs on AWS touch few rows, so a slow-burn defect can stay under
  it; comparing against a rolling baseline is the next step.
* On AWS, lineage comes from the published graph (OpenLineage events do not outlive a Lambda
  invocation) and quality results are scoped by the execution's time window.
