# ADR 0003: Quarantine bad rows in silver, fail fast in gold

**Status:** accepted

## Context
Real EIA feeds contain missing hours, malformed values, sign errors and respondents that
are not in the reference data. Failing the pipeline for a handful of bad rows blocks every
consumer; silently dropping them hides upstream problems.

## Decision
* Silver: error-severity row-level failures are diverted to `quality.quarantine` (verbatim
  record plus failing rule names) and the rest flows. Dataset-level failures raise.
* Gold: `strict` mode. Any error-level failure is a bug in our own transformation, so the
  step fails before anything is published, and lineage records a `FAIL` event.
* `warn` rules are recorded and never block.

## Consequences
* Stewards get a replayable queue instead of a log line.
* A gold data product is never partially wrong. It is either refreshed or left at its
  last good snapshot.
