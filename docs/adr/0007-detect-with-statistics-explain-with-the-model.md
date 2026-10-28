# ADR 0007: The anomaly watcher detects with statistics and explains with the model

**Status:** accepted

## Context
A daily brief on what changed on the grid is read by people who act on it: operations, trading,
sustainability reporting. Two things go wrong when a model is asked to find anomalies itself.
It is not repeatable (the same data can give a different list tomorrow), and a number it writes
can be wrong in a way that reads exactly like a right one. What a model is good at is the part
people find slow: pulling together what else moved that day and saying it plainly.

## Decision
* **Detection is plain statistics** (`watcher.detect`): each balancing authority's day against
  its own recent history, with a robust z-score (median and MAD, with a floor on the scale) and
  a minimum move per signal. Demand is compared per reported hour and with the same day type,
  so missing hours and weekends do not raise alerts; fuel shares are judged by their one-day
  move, because wind and solar levels swing widely over a week.
* **The model only explains** what detection found, with read-only tools: one that gathers the
  evidence for an anomaly (peers in the interconnection, forecast error, fuels that moved, the
  export/import position) and the analyst's SQL and knowledge-graph tools.
* **The numbers come from code.** The brief's table is rendered from the detection, not from
  model output. A quiet day does not call a model at all.
* **The brief is public.** It goes to a wide audience, so the watcher runs with `public`
  clearance, its tools refuse anything above it, and on AWS Lake Formation gives its role
  `SELECT` on public gold tables only.
* Detection thresholds live in code (`SIGNALS`) and are documented in every brief, so a reader
  knows what "unusual" meant.

## Consequences
* The same gold data always gives the same anomalies, which makes the watcher testable: planted
  events must be found and the day before must be quiet.
* A model outage or a bad answer degrades the wording of a brief, never its list of changes.
  The deterministic `OfflineBriefWriter` is a usable fallback.
* Statistics cannot tell a real event from a data problem on their own. Days with too few hours
  reported are skipped and listed as data gaps, and the brief points to `gridlake triage`.
* Thresholds were tuned on the synthetic feed. Real EIA-930 data needs a longer history and may
  need per-BA thresholds; that is a configuration change, not a redesign.
