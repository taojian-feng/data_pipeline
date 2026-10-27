"""A deterministic stand-in for the model that drives the data reliability agent.

Like the grid analyst's ``OfflinePlanner`` it is not an LLM. It works an incident the way the
instructions ask Claude to (run log, quality history, lineage, contract, rejected rows), then
applies a small decision table to the evidence:

* a numeric rule broken by values about 1000x (or 1/1000x) the same entity's accepted values,
  or by negative values: the source changed units or sign, so an issue goes to the source owner;
* a referential rule broken by codes EIA publishes that the reference seed lacks: a seed fix;
* values just past a range bound, close to the accepted ones: the contract is out of date, so
  a fix raises the bound and bumps the contract version;
* a gold gate failure with clean inputs: a transformation bug, so an issue to the owner;
* a failed step without a gate: an outage or a platform fault, by the error text.

Anything it cannot place is diagnosed ``unknown`` with low confidence and goes to a person.
"""

from __future__ import annotations

import json
import math
import re
from typing import Any

from gridlake.agent.llm import OfflinePlanner, Reply

# Energy-source codes EIA reports beyond the eight in seeds/fuel_types.csv, as
# (name, category, is_renewable, illustrative direct kg CO2/MWh). Confirm against EIA-930.
EIA_ENERGY_SOURCES = {
    "BAT": ("Battery Storage", "Storage", "false", "0"),
    "PS": ("Pumped Storage", "Storage", "false", "0"),
    "GEO": ("Geothermal", "Renewable", "true", "0"),
    "SNB": ("Solar with Battery Storage", "Renewable", "true", "0"),
    "WNB": ("Wind with Battery Storage", "Renewable", "true", "0"),
}
_OUTAGE = re.compile(
    r"HTTP|\b5\d\d\b|timed? ?out|Timeout|Connection|failed after|Unavailable", re.I
)
_PLATFORM = re.compile(r"AccessDenied|not authorized|Throttl|KMS|Lake Formation|NoSuchBucket", re.I)
_SCHEMA = re.compile(r"ContractError|missing columns|Schema|cannot cast|type mismatch", re.I)


def incident_from_prompt(prompt: str) -> dict[str, Any]:
    m = re.search(r"```json\n(.*?)\n```", prompt, re.S)
    return json.loads(m.group(1)) if m else {}


def _nice_ceiling(x: float) -> int:
    exp = 10 ** math.floor(math.log10(x))
    return int(next(m * exp for m in (1, 2, 2.5, 5, 10) if m * exp >= x))


def _bump_minor(version: str) -> str:
    parts = [int(p) for p in re.findall(r"\d+", version)[:3]] + [0, 0, 0]
    return f"{parts[0]}.{parts[1] + 1}.0"


class OfflineResponder:
    name = "offline-responder"

    def respond(
        self, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> Reply:
        inc = incident_from_prompt(messages[0]["content"])
        history = OfflinePlanner._history(messages)
        call = self._next_call(inc, history)
        if call is not None:
            name, args = call
            return Reply(
                "tool_use",
                [
                    {
                        "type": "tool_use",
                        "id": f"call_{len(history) + 1}",
                        "name": name,
                        "input": args,
                    }
                ],
            )
        return Reply("end_turn", [{"type": "text", "text": self._summary(inc, history)}])

    # -- plan -------------------------------------------------------------------
    @staticmethod
    def _result(history: list, name: str, **match: Any) -> dict[str, Any] | None:
        for n, args, result, err in history:
            if n == name and all(args.get(k) == v for k, v in match.items()):
                return {"error": result.get("error")} if err else result
        return None

    def _next_call(self, inc: dict[str, Any], history: list) -> tuple[str, dict] | None:
        got = lambda name, **m: self._result(history, name, **m)  # noqa: E731
        if got("get_run_log") is None:
            return "get_run_log", {}
        table, rule = inc.get("table"), inc.get("rule") or ""
        if table:
            for name in ("get_quality_results", "get_lineage", "get_contract"):
                if got(name, table=table) is None:
                    return name, {"table": table}
            kind = rule.split("(")[0]
            if kind in ("range", "not_null", "referential", "accepted_values") and (
                got("inspect_quarantine", table=table) is None
            ):
                return "inspect_quarantine", {"table": table, "rule": rule, "limit": 3}
            if kind == "referential" and got("record_diagnosis") is None:
                ref = re.search(r"->([a-z_]+\.[a-z0-9_]+)\.", rule)
                if ref and got("get_contract", table=ref.group(1)) is None:
                    return "get_contract", {"table": ref.group(1)}
                seed = (
                    (got("get_contract", table=ref.group(1)) or {}).get("seed_file")
                    if ref
                    else None
                )
                if seed and got("read_repo_file", path=seed) is None:
                    return "read_repo_file", {"path": seed}
        if got("record_diagnosis") is None:
            return "record_diagnosis", self._diagnose(inc, history)["diagnosis"]
        if not any(h[0] in ("draft_issue", "draft_fix") for h in history):
            return self._diagnose(inc, history)["draft"]
        fix = next((h for h in history if h[0] == "draft_fix"), None)
        if fix and fix[3] and not any(h[0] == "draft_issue" for h in history):
            # The fix did not validate: hand it to a person instead.
            d = self._diagnose(inc, history)["diagnosis"]
            title = f"{inc.get('table')}: {d['category'].replace('_', ' ')} needs a person"
            return "draft_issue", self._issue(
                inc, d, history, title, f"\n\nThe drafted fix was refused: {fix[2]['error']}"
            )
        return None

    # -- decide -----------------------------------------------------------------
    def _diagnose(self, inc: dict[str, Any], history: list) -> dict[str, Any]:
        """{"diagnosis": record_diagnosis input, "draft": (tool, input)} from the evidence."""
        if not inc.get("table"):
            return self._step_failure(inc, history)
        got = lambda name, **m: self._result(history, name, **m)  # noqa: E731
        table, rule = inc["table"], inc.get("rule") or ""
        kind, layer = rule.split("(")[0], table.split(".")[0]
        lineage = got("get_lineage", table=table) or {}
        owner = lineage.get("owner")
        source = [u for u in lineage.get("upstream", []) if u["dataset"].startswith("bronze.")]
        source_owner = source[0]["owner"] if source else owner
        evidence: list[str] = []
        for r in (got("get_quality_results", table=table) or {}).get("rules", []):
            if r["rule"] == rule:
                h = r["history"]
                base = (
                    f"{h['failure_rate']:.2%} across {h['earlier_evaluations']} earlier runs"
                    if isinstance(h, dict)
                    else "this is the rule's first run"
                )
                evidence.append(
                    f"{rule} rejected {r['failed_rows']} of {r['total_rows']} rows "
                    f"({r['failure_rate']:.2%}); baseline: {base}."
                )
        q = got("inspect_quarantine", table=table) or {}
        rej, same = q.get("rejected_profile") or {}, q.get("same_entity") or {}
        who = f"{same['key']}={same['value']}" if same else None
        if same and same["share_of_rejected_rows"] >= 0.5:
            evidence.append(f"{same['share_of_rejected_rows']:.0%} of the rejected rows are {who}.")
        span = next(
            (
                v
                for k, v in {**q, **same}.items()
                if k.startswith("rejected_") and k.endswith("_range")
            ),
            None,
        )
        if span:
            evidence.append(f"Rejected rows of {who or table} run from {span[0]} to {span[1]}.")
        upstream_bad = [
            u
            for u in lineage.get("upstream", [])
            if any(r["failure_rate"] >= 0.02 for r in u.get("failed_error_rules_this_run", []))
        ]

        def plan(category, cause, confidence, title, fix=None, owner_=owner):
            d = {
                "category": category,
                "cause": cause,
                "evidence": evidence,
                "confidence": confidence,
            }
            if owner_:
                d["owner"] = owner_
            draft = (
                ("draft_fix", fix) if fix else ("draft_issue", self._issue(inc, d, history, title))
            )
            return {"diagnosis": d, "draft": draft}

        if inc.get("kind") == "step_failed" and layer == "gold":
            evidence.append(
                "Every input stayed within its usual error-rule failure rate in this run."
                if not upstream_bad
                else f"Inputs that also failed: {[u['dataset'] for u in upstream_bad]}."
            )
            return plan(
                "transformation_bug",
                f"The strict gate on {table} rejected the batch ({rule}) although its inputs were "
                "clean, so the gold transformation produced invalid rows. Nothing was published "
                "and consumers still see the previous snapshot.",
                "medium" if upstream_bad else "high",
                f"{table} gate failed on {rule}: likely a transformation bug",
            )

        if kind == "range" and "median" in rej:
            col = q.get("column")
            r_med = (same.get("rejected") or rej)["median"]
            a_med = (same.get("accepted") or q.get("accepted_profile") or {}).get("median")
            bound = re.search(r":(-?[\d.]+)\.\.(-?[\d.]+)\)", rule)
            lo, hi = (float(bound.group(1)), float(bound.group(2))) if bound else (None, None)
            ratio = r_med / a_med if a_med else None
            if ratio is not None:
                evidence.append(
                    f"Rejected {col} median {r_med:,.0f} vs {a_med:,.0f} in the accepted rows of "
                    f"{who or 'the table'} (x{ratio:,.1f})."
                )
            if ratio is not None and (abs(ratio) >= 100 or 0 < abs(ratio) <= 0.01):
                factor = 1000 if 300 <= abs(ratio) <= 3000 else round(abs(ratio))
                units = " (most likely kWh instead of MWh)" if factor == 1000 else ""
                return plan(
                    "source_data_defect",
                    f"The source started sending {col} for {who or 'some rows'} about {factor:,}x "
                    f"larger than before{units}. The rule is right: the rows are quarantined, so "
                    f"{table} and everything downstream is missing them.",
                    "high",
                    f"{who or table}: {col} arrives ~{factor:,}x too large"
                    + (f" since {span[0][:13]}" if span else ""),
                    owner_=source_owner,
                )
            if lo is not None and rej["max"] < lo:
                return plan(
                    "source_data_defect",
                    f"The source sent {col} values below {lo:g} for {who or 'some rows'} "
                    "(a sign error upstream). The rule is right.",
                    "high",
                    f"{who or table}: negative {col} from the source",
                    owner_=source_owner,
                )
            if (
                hi is not None
                and layer != "gold"
                and hi < rej["min"] <= hi * 1.5
                and ratio is not None
                and ratio < 3
            ):
                new_max = _nice_ceiling(rej["max"] * 1.25)
                return plan(
                    "contract_out_of_date",
                    f"{col} values just above the {hi:,.0f} bound match the accepted values of "
                    f"{who or 'the same entities'}, so the bound is out of date, not the data.",
                    "medium",
                    "",
                    fix=self._raise_bound(inc, history, hi, new_max),
                )

        if kind == "referential":
            values = [v for v, _ in rej.get("top_values", [])]
            ref = re.search(r"->([a-z_]+\.[a-z0-9_]+)\.", rule)
            ref_table = ref.group(1) if ref else "the reference table"
            ref_contract = got("get_contract", table=ref_table) if ref else None
            seed = (ref_contract or {}).get("seed_file")
            evidence.append(f"Rejected codes missing from {ref_table}: {', '.join(values)}.")
            if (
                values
                and seed == "seeds/fuel_types.csv"
                and all(v in EIA_ENERGY_SOURCES for v in values)
            ):
                evidence.append(
                    f"{', '.join(values)} are EIA energy-source codes; {seed} lacks them."
                )
                return plan(
                    "reference_data_gap",
                    f"EIA now reports energy source {', '.join(values)}, which {ref_table} does not "
                    "list yet, so those rows are quarantined. The data is valid; the reference seed "
                    "is behind.",
                    "high",
                    "",
                    fix=self._seed_rows(history, seed, values, table),
                    owner_=self._owner_of(ref_contract) or owner,
                )
            return plan(
                "reference_data_gap",
                f"{table} received codes {', '.join(values)} that {ref_table} does not know. "
                "Someone has to confirm whether they are real and register them.",
                "medium",
                f"{table}: unknown codes {', '.join(values[:3])} in {rule.split('(')[1].split('-')[0]}",
            )

        if kind == "not_null":
            return plan(
                "source_data_defect",
                f"The source delivered missing or malformed values for {table} at an unusual rate.",
                "medium",
                f"{table}: missing values from the source ({rule})",
                owner_=source_owner,
            )
        if kind == "unique":
            return plan(
                "transformation_bug",
                f"{table} has duplicate keys, which deduplication in the transformation should prevent.",
                "medium",
                f"{table}: duplicate keys ({rule})",
            )
        return plan(
            "unknown",
            f"{rule} failed on {table}; the evidence does not point to a single cause.",
            "low",
            f"{table}: {rule} failed",
        )

    def _step_failure(self, inc: dict[str, Any], history: list) -> dict[str, Any]:
        error, step = inc.get("error") or "", inc.get("step") or "?"
        recent = (self._result(history, "get_run_log") or {}).get("recent_runs", [])
        evidence = [f"Step {step} failed: {error}"]
        later = [
            s["name"]
            for s in (self._result(history, "get_run_log") or {}).get("steps", [])
            if s["status"] != "COMPLETE" and s["name"] != step
        ]
        evidence.append(
            "No later step ran, so every table keeps its previous snapshot."
            if not later
            else f"Also incomplete: {', '.join(later)}."
        )
        ok_before = [r for r in recent if r["status"] == "SUCCEEDED"]
        if ok_before:
            evidence.append(f"{len(ok_before)} of the last {len(recent)} runs succeeded.")
        ingest = step.lower().startswith("ingest")
        if _SCHEMA.search(error):
            cat, cause, conf = (
                "schema_drift",
                f"{step} received data that no longer matches its contract.",
                "medium",
            )
        elif _PLATFORM.search(error):
            cat, cause, conf = (
                "infrastructure",
                f"{step} was refused by the platform (permissions or limits), not by the data.",
                "medium",
            )
        elif _OUTAGE.search(error):
            cat = "upstream_outage" if ingest else "infrastructure"
            cause = (
                f"{step} could not reach its source after retries. Bronze landing is idempotent, "
                "so re-running the same window is safe once the source is back."
            )
            conf = "high" if ingest else "medium"
        else:
            cat, cause, conf = (
                "unknown",
                f"{step} failed with an error the playbook does not cover.",
                "low",
            )
        d = {"category": cat, "cause": cause, "evidence": evidence, "confidence": conf}
        title = f"{step} failed: {error.split(':', 1)[-1].strip()[:80]}"
        return {"diagnosis": d, "draft": ("draft_issue", self._issue(inc, d, history, title))}

    # -- drafts -----------------------------------------------------------------
    @staticmethod
    def _owner_of(contract: dict[str, Any] | None) -> str | None:
        m = re.search(r"^owner: (\S+)", (contract or {}).get("yaml", ""), re.M)
        return m.group(1) if m else None

    def _issue(
        self, inc: dict[str, Any], d: dict[str, Any], history: list, title: str, extra: str = ""
    ) -> dict:
        lineage = self._result(history, "get_lineage", table=inc.get("table")) or {}
        downstream = [x["dataset"] for x in lineage.get("downstream", [])]
        consumers = sorted(
            {c for x in lineage.get("downstream", []) for c in x.get("consumers", [])}
        )
        next_steps = {
            "source_data_defect": "- Ask the source to correct and restate the affected hours; do not relax the rule.\n- Restated hours land as a new batch and silver recomputes the keys they touch.",
            "transformation_bug": "- Reproduce with `gridlake run` on the run's window and fix the transformation.\n- Do not relax the gold rule: the gate is doing its job.",
            "upstream_outage": "- Check the source's status, then re-run the same window: bronze landing is idempotent.",
            "infrastructure": "- Check the permissions and service quotas named in the error, then re-run.",
            "schema_drift": "- Compare the delivered fields with the contract and agree a versioned change with the producer.",
            "reference_data_gap": "- Confirm the new codes, add them to the reference seed in a pull request, then `gridlake replay` the table.",
        }.get(d["category"], "- Investigate with the run log and the quarantined rows.")
        if inc.get("kind") == "step_failed" and not inc.get("table"):
            what, impact = (
                inc.get("error"),
                "The run stopped; later steps did not run, so every table still serves its previous snapshot.",
            )
        else:
            what = f"`{inc['rule']}` rejected {inc['failed_rows']} of {inc['total_rows']} rows of `{inc['table']}`."
            impact = f"Downstream: {', '.join(downstream) or 'none recorded'}." + (
                f" Consumers: {', '.join(consumers)}." if consumers else ""
            )
        body = (
            f"### What happened\n{what}\n\n### Likely cause\n{d['cause']}\n\n### Evidence\n"
            + "\n".join(f"- {e}" for e in d["evidence"])
            + f"\n\n### Impact\n{impact}\n\n### Suggested next steps\n{next_steps}{extra}"
            + (f"\n\nOwner: {d['owner']}" if d.get("owner") else "")
        )
        labels = {
            "source_data_defect": ["data-quality", "upstream"],
            "transformation_bug": ["bug"],
            "upstream_outage": ["upstream", "ops"],
            "infrastructure": ["ops"],
        }.get(d["category"], ["triage"])
        return {"title": title[:120], "body": body, "labels": labels}

    def _seed_rows(self, history: list, seed: str, codes: list[str], table: str) -> dict[str, Any]:
        text = (self._result(history, "read_repo_file", path=seed) or {}).get("text", "")
        last = [line for line in text.splitlines() if line.strip()][-1]
        rows = [",".join([c, *EIA_ENERGY_SOURCES[c]]) for c in codes]
        return {
            "path": seed,
            "edits": [{"old": last + "\n", "new": last + "\n" + "\n".join(rows) + "\n"}],
            "title": f"Register EIA energy source(s) {', '.join(codes)} in {seed}",
            "rationale": (
                f"EIA reports generation for {', '.join(codes)}, which the fuel-type dimension does not "
                "list, so those rows are quarantined from silver.generation_hourly and missing from "
                "the gold fuel mix. This adds them with their EIA names and categories. The emission "
                "factor is direct emissions (0 for storage and geothermal, as for other "
                "non-combustion sources); the dimension owner should confirm it. After merging, "
                "run `gridlake run` (to reload the seed) and then `gridlake replay "
                f"{table}` to publish the quarantined rows and rebuild the gold dates they touch."
            ),
        }

    def _raise_bound(
        self, inc: dict[str, Any], history: list, old_max: float, new_max: int
    ) -> dict[str, Any]:
        table = inc["table"]
        contract = self._result(history, "get_contract", table=table) or {}
        text, column = contract.get("yaml", ""), inc["rule"].split("(")[1].split(":")[0]
        line = next(
            ln
            for ln in text.splitlines()
            if f"rule: range, column: {column}," in ln and "severity: error" in ln
        )
        old_bound = f"max: {old_max:g}" if f"max: {old_max:g}" in line else f"max: {int(old_max)}"
        version = contract.get("version", "1.0.0")
        return {
            "path": contract.get("path", f"contracts/{table}.yaml"),
            "edits": [
                {"old": line, "new": line.replace(old_bound, f"max: {new_max}")},
                {"old": f"version: {version}\n", "new": f"version: {_bump_minor(version)}\n"},
            ],
            "title": f"Raise the {column} bound on {table} to {new_max:,}",
            "rationale": (
                f"Legitimate {column} values now exceed the {old_max:,.0f} bound. This raises it to "
                f"{new_max:,} (25% headroom over the largest rejected value) and bumps the contract "
                "to a new minor version. The owner should confirm the new bound."
            ),
        }

    # -- answer -----------------------------------------------------------------
    def _summary(self, inc: dict[str, Any], history: list) -> str:
        diag = next((h[1] for h in history if h[0] == "record_diagnosis" and not h[3]), None)
        if diag is None:
            return f"I could not reach a diagnosis for {inc.get('id')}."
        drafts = [
            (h[0], h[2]) for h in history if h[0] in ("draft_issue", "draft_fix") and not h[3]
        ]
        text = f"{diag['category']} ({diag['confidence']} confidence): {diag['cause']}"
        for name, result in drafts:
            kind = "fix" if name == "draft_fix" else "issue"
            text += f" Drafted {kind} {result['draft_id']} for approval."
        return text
