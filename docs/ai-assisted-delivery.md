# AI-assisted delivery and validating generated code

This repository was built with an AI pair programmer (Claude Code). The point is not that
AI wrote code. The point is the controls that make AI-written pipeline code safe to ship,
which matter most in regulated environments.

| Risk with generated pipeline code | Control in this repo |
|---|---|
| Plausible SQL with subtly wrong semantics (leakage, double counting) | Property tests on the semantics: `test_forecast_features_are_point_in_time_correct`, `test_redelivered_stream_events_do_not_double_count`, `test_restatement_flows_through_to_gold` |
| Schema drift between code, catalog and docs | Contracts are the single source; Terraform and Python read the same YAML |
| Silent data corruption reaching consumers | Quality gates with quarantine (silver) and circuit breaker (gold) |
| Unreviewable "magic" | Small modules, ADRs for each decision, lineage for every run |
| Over-permissive infrastructure | Least-privilege IAM, Lake Formation tag-based grants, KMS everywhere, TLS-only buckets; `terraform validate` in CI |
| Agents reasoning over invented relationships | Ontology domain/range validation on every triple |

Working method: write the contract and the tests for the behaviour first, let the assistant
draft the implementation, then review against the tests, the lineage output and the
quality report from a real run. Generated code is never merged on "looks right".
