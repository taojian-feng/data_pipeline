"""A deterministic stand-in for the model that drives the self-service data product agent.

Like the other offline planners it is not an LLM. It covers the requests that come up most,
period roll-ups of the gold products, and works them the way the instructions ask Claude to:
list the tables, read the source contract, preview the SQL, then draft. It reads

* the measure family: energy (demand, peak, generation, emissions) from
  ``gold.ba_daily_energy``; renewable share, recomputed from generation, from
  ``gold.fuel_mix_daily`` and ``silver.dim_fuel_type``; or frequency excursions from
  ``gold.grid_reliability_daily``;
* the period: daily, weekly (the default) or monthly;
* the entity: balancing authority (the default), region or interconnection.

Classification is the floor its inputs imply, or the requested level when that is stricter.
Anything else gets an explanation instead of a draft; ``--llm bedrock`` handles open requests.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from gridlake.agent.llm import OfflinePlanner, Reply
from gridlake.contracts import CLASSIFICATIONS

_GRAINS = (
    ("day", r"\bdaily\b|\bper day\b|\bby day\b|\beach day\b"),
    ("month", r"\bmonth"),
    ("week", r"\bweek"),
)
_PERIOD = {
    "day": ("date", "daily", 1),
    "week": ("week_start", "weekly", 7),
    "month": ("month_start", "monthly", 31),
}
_GROUPS = (
    ("interconnection", r"interconnect"),
    ("region", r"\bregion"),
    ("ba", r""),
)
_CONSUMERS = (
    (r"dashboard|\bbi\b|tableau|quicksight|power bi", "bi-dashboards"),
    (r"sustainab|esg|report", "sustainability-reporting"),
    (r"forecast|machine learning|\bml\b|model", "ml-features"),
    (r"reliab|operations|engineer", "reliability-engineering"),
)


@dataclass
class Measure:
    column: str
    expr: str
    type: str
    description: str
    nullable: bool = True
    business_term: str | None = None


@dataclass
class Plan:
    family: str
    source: str
    joins: list[str]
    grain: str
    group: str
    measures: list[Measure]
    rules: list[dict[str, Any]]
    reconcile: tuple[str, str]  # product column, its daily expression over the source
    topic: str = ""
    notes: list[str] = field(default_factory=list)

    @property
    def period(self) -> str:
        return _PERIOD[self.grain][0]

    @property
    def name(self) -> str:
        return f"gold.{self.group}_{_PERIOD[self.grain][1]}_{self.family}"

    def group_columns(self) -> list[tuple[str, str, str]]:
        """(column, expression, description) for the entity key."""
        if self.group == "ba":
            cols = [("ba_code", "s.ba_code", "EIA balancing authority code")]
            if self.family == "energy":
                cols.append(("ba_name", "s.ba_name", "Balancing authority name"))
            return cols
        where = "s" if self.family == "energy" else "b"
        if self.group == "region":
            return [
                ("region_name", f"{where}.region_name", "EIA region of the balancing authorities")
            ]
        return [("interconnection", f"{where}.interconnection", "North American interconnection")]

    def period_expr(self, alias: str = "s") -> str:
        if self.grain == "day":
            return f"{alias}.date"
        return f"CAST(date_trunc('{self.grain}', {alias}.date) AS DATE)"

    def sql(self) -> str:
        keys = self.group_columns()
        select = [f"{self.period_expr()} AS {self.period}"]
        select += [f"{expr} AS {col}" for col, expr, _ in keys]
        select += [f"{m.expr} AS {m.column}" for m in self.measures]
        groups = ", ".join(str(i) for i in range(1, len(keys) + 2))
        return (
            "SELECT "
            + ",\n       ".join(select)
            + f"\nFROM {self.source} s"
            + "".join(f"\n{j}" for j in self.joins)
            + f"\nGROUP BY {groups}\nORDER BY {groups}\n"
        )

    def columns(self) -> list[dict[str, Any]]:
        label = {
            "day": "UTC day",
            "week": "first day (Monday) of the UTC week",
            "month": "first day of the UTC month",
        }
        out = [
            {
                "name": self.period,
                "type": "date",
                "description": label[self.grain][0].upper() + label[self.grain][1:],
            }
        ]
        out += [
            {"name": col, "type": "string", "description": desc}
            for col, _, desc in self.group_columns()
        ]
        for m in self.measures:
            entry: dict[str, Any] = {"name": m.column, "type": m.type, "description": m.description}
            if not m.nullable:
                entry["nullable"] = False
            if m.business_term:
                entry["business_term"] = m.business_term
            out.append(entry)
        return out

    def checks(self, table: str) -> dict[str, str]:
        """Reconcile one additive measure with the input, and check no period goes missing."""
        key, key_expr, _ = self.group_columns()[0]
        on = f"{self.period}, {key}"

        def per_period(agg: str) -> str:
            return (
                f"(\n    SELECT {self.period_expr()} AS {self.period}, {key_expr} AS {key},\n"
                f"           {agg}\n"
                f"    FROM {self.source} s"
                + "".join(f"\n    {j}" for j in self.joins)
                + "\n    GROUP BY 1, 2\n)"
            )

        col, expr = self.reconcile
        return {
            f"{col} reconciles with {self.source}": (
                f"SELECT p.{self.period}, p.{key}\n"
                f"FROM {table} p\n"
                f"JOIN {per_period(f'sum({expr}) AS expected')} i USING ({on})\n"
                f"WHERE abs(coalesce(p.{col}, 0) - coalesce(i.expected, 0)) > 0.01"
            ),
            f"every {key} and {self.period} in {self.source} is present": (
                f"SELECT i.{self.period}, i.{key}\n"
                f"FROM {per_period('count(*) AS n')} i\n"
                f"LEFT JOIN {table} p USING ({on})\n"
                f"WHERE p.{self.period} IS NULL"
            ),
        }


def _energy(q: str, grain: str, group: str) -> Plan:
    unit = grain
    want = {
        "demand": bool(re.search(r"demand|load|consumption|energy", q)),
        "peak": bool(re.search(r"peak", q)),
        "generation": bool(re.search(r"generation|energy", q)),
        "emissions": bool(re.search(r"emission|carbon|co2", q)),
    }
    if not any(want.values()):
        want["demand"] = True
    notes = []
    if want["peak"] and group != "ba":
        want["peak"] = False
        notes.append(
            f"I left out peak demand: a {group}'s peak is the highest hour of the summed demand, "
            "which daily per-BA peaks cannot give; that needs silver.demand_hourly"
        )
    measures, rules = [], []
    if want["demand"] or want["peak"]:
        measures.append(
            Measure(
                "total_demand_mwh",
                "sum(s.total_demand_mwh)",
                "double",
                f"Total demand in the {unit}, MWh",
                False,
            )
        )
        rules.append({"rule": "range", "column": "total_demand_mwh", "min": 0})
    if want["peak"]:
        measures.append(
            Measure(
                "peak_demand_mwh",
                "max(s.peak_demand_mwh)",
                "double",
                f"Highest hourly demand in the {unit}, MWh",
                False,
                "Peak Demand",
            )
        )
        rules.append(
            {
                "rule": "expression",
                "sql": "peak_demand_mwh <= total_demand_mwh",
                "description": "A peak hour cannot exceed the period total",
            }
        )
    if want["generation"]:
        measures.append(
            Measure(
                "net_generation_mwh",
                "sum(s.net_generation_mwh)",
                "double",
                f"Net generation in the {unit}, MWh",
            )
        )
    if want["emissions"]:
        measures.append(
            Measure(
                "total_emissions_t",
                "sum(s.total_emissions_t)",
                "double",
                f"CO2 emissions from generation in the {unit}, tonnes",
                True,
                "Emissions",
            )
        )
    measures.append(
        Measure(
            "days_reported",
            "CAST(count(*) AS INTEGER)",
            "int",
            "Balancing-authority days with data in the period",
            False,
        )
    )
    if group == "ba":
        measures.append(
            Measure(
                "hours_reported",
                "CAST(sum(s.hours_reported) AS INTEGER)",
                "int",
                "Hours with demand data in the period",
                False,
            )
        )
    first = measures[0]
    expr = {
        "total_demand_mwh": "s.total_demand_mwh",
        "net_generation_mwh": "s.net_generation_mwh",
        "total_emissions_t": "s.total_emissions_t",
    }
    labels = [k if k != "peak" else "peak demand" for k, v in want.items() if v]
    topic = labels[0] if len(labels) == 1 else ", ".join(labels[:-1]) + " and " + labels[-1]
    return Plan(
        "energy",
        "gold.ba_daily_energy",
        [],
        grain,
        group,
        measures,
        rules,
        (first.column, expr[first.column]),
        topic,
        notes,
    )


def _renewables(q: str, grain: str, group: str) -> Plan:
    unit = grain
    joins = ["JOIN silver.dim_fuel_type f ON f.fuel_type = s.fuel_type"]
    if group != "ba":
        joins.append("JOIN silver.dim_balancing_authority b ON b.ba_code = s.ba_code")
    pos = "greatest(s.generation_mwh, 0)"
    measures = [
        Measure(
            "renewable_share_pct",
            f"round(100 * sum(CASE WHEN f.is_renewable THEN {pos} ELSE 0 END) / nullif(sum({pos}), 0), 2)",
            "double",
            f"Solar + wind + hydro as % of positive generation in the {unit}, recomputed from generation",
            True,
            "Renewable Share",
        ),
        Measure(
            "renewable_generation_mwh",
            f"sum(CASE WHEN f.is_renewable THEN {pos} ELSE 0 END)",
            "double",
            f"Generation from renewable sources in the {unit}, MWh",
            False,
        ),
        Measure(
            "positive_generation_mwh",
            f"sum({pos})",
            "double",
            f"Positive generation from all sources in the {unit}, MWh",
            False,
        ),
    ]
    rules = [
        {"rule": "range", "column": "renewable_share_pct", "min": 0, "max": 100},
        {
            "rule": "expression",
            "sql": "renewable_generation_mwh <= positive_generation_mwh + 0.001",
            "description": "Renewables are part of the total",
        },
    ]
    return Plan(
        "renewable_share",
        "gold.fuel_mix_daily",
        joins,
        grain,
        group,
        measures,
        rules,
        ("positive_generation_mwh", pos),
        "renewable share",
    )


def _reliability(q: str, grain: str, group: str) -> Plan:
    unit = grain
    joins = (
        [] if group == "ba" else ["JOIN silver.dim_balancing_authority b ON b.ba_code = s.ba_code"]
    )
    measures = [
        Measure(
            "excursion_minutes",
            "sum(s.excursion_minutes)",
            "long",
            f"Substation-minutes outside 59.95-60.05 Hz in the {unit}",
            False,
            "Frequency Excursion",
        ),
        Measure(
            "minutes_observed",
            "sum(s.minutes_observed)",
            "long",
            f"Substation-minutes observed in the {unit}",
            False,
        ),
        Measure(
            "min_frequency_hz",
            "min(s.min_frequency_hz)",
            "double",
            f"Lowest frequency seen in the {unit}, Hz",
        ),
        Measure(
            "max_frequency_hz",
            "max(s.max_frequency_hz)",
            "double",
            f"Highest frequency seen in the {unit}, Hz",
        ),
        Measure(
            "days_reported",
            "CAST(count(*) AS INTEGER)",
            "int",
            "Balancing-authority days with telemetry in the period",
            False,
        ),
    ]
    rules = [
        {
            "rule": "expression",
            "sql": "excursion_minutes <= minutes_observed",
            "description": "Excursions are a subset of observed minutes",
        }
    ]
    return Plan(
        "frequency_excursions",
        "gold.grid_reliability_daily",
        joins,
        grain,
        group,
        measures,
        rules,
        ("minutes_observed", "s.minutes_observed"),
        "frequency excursions",
    )


def plan(text: str) -> Plan | None:
    q = text.lower()
    grain = next(g for g, rx in (*_GRAINS, ("week", r"")) if re.search(rx, q))
    group = next(g for g, rx in _GROUPS if re.search(rx, q))
    if re.search(r"frequency|excursion|reliab", q):
        p = _reliability(q, grain, group)
    elif re.search(r"renewable|clean energy|green", q):
        p = _renewables(q, grain, group)
    elif re.search(r"demand|load|consumption|energy|peak|generation|emission|carbon|co2", q):
        p = _energy(q, grain, group)
    else:
        return None
    if any(m.column == "days_reported" for m in p.measures):
        # One row per BA-day, so a BA has at most a period's days; a region has more.
        bound = {"max": _PERIOD[grain][2]} if group == "ba" else {}
        p.rules.append({"rule": "range", "column": "days_reported", "min": 1, **bound})
    return p


def parse_prompt(prompt: str) -> dict[str, str]:
    out = {}
    for key, label in (
        ("request", "Request"),
        ("owner", "Requested by"),
        ("name", "Name"),
        ("consumers", "Consumers"),
        ("classification", "Classification asked for"),
    ):
        m = re.search(rf"^{label}: (.*)$", prompt, re.M)
        if m:
            out[key] = m.group(1).strip()
    return out


def asked_classification(fields: dict[str, str]) -> str | None:
    if fields.get("classification") in CLASSIFICATIONS:
        return fields["classification"]
    q = fields.get("request", "").lower()
    for level in ("confidential", "internal", "public"):
        if re.search(rf"\b{level}\b", q):
            return level
    if re.search(r"\bexternal|publish(ed)? (it )?(on|to) the (web|website)|open data", q):
        return "public"
    return None


def consumers(fields: dict[str, str]) -> list[str]:
    if fields.get("consumers"):
        return [c.strip() for c in fields["consumers"].split(",") if c.strip()]
    q = fields.get("request", "").lower()
    return [name for rx, name in _CONSUMERS if re.search(rx, q)] or ["analytics"]


class OfflineDesigner:
    name = "offline-designer"

    def respond(
        self, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> Reply:
        fields = parse_prompt(messages[0]["content"])
        history = OfflinePlanner._history(messages)
        call = self._next_call(fields, history)
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
        return Reply("end_turn", [{"type": "text", "text": self._summary(fields, history)}])

    @staticmethod
    def _last(history: list, tool: str) -> tuple[dict, Any, bool] | None:
        for name, args, result, err in reversed(history):
            if name == tool:
                return args, result, err
        return None

    def _next_call(self, fields: dict[str, str], history: list) -> tuple[str, dict] | None:
        done = [h[0] for h in history]
        if not done:
            return "list_tables", {}
        p = plan(fields.get("request", ""))
        if p is None or any(h[3] for h in history):
            return None
        listed = {t["table"] for t in history[0][2].get("tables", [])}
        if p.source not in listed:
            return None
        if "get_contract" not in done:
            return "get_contract", {"table": p.source}
        if "preview_transform" not in done:
            return "preview_transform", {"sql": p.sql()}
        if "draft_data_product" not in done:
            preview = self._last(history, "preview_transform")[1]
            return "draft_data_product", self._draft(fields, p, preview)
        return None

    @staticmethod
    def _classification(fields: dict[str, str], preview: dict) -> str:
        floor, asked = preview["minimum_classification"], asked_classification(fields)
        if asked and CLASSIFICATIONS.index(asked) > CLASSIFICATIONS.index(floor):
            return asked
        return floor

    def _draft(self, fields: dict[str, str], p: Plan, preview: dict) -> dict[str, Any]:
        cls = self._classification(fields, preview)
        who = consumers(fields)
        group_label = {
            "ba": "balancing authority",
            "region": "EIA region",
            "interconnection": "interconnection",
        }[p.group]
        inputs = ", ".join(preview["inputs"])
        rationale = [
            f"Rolled up from {inputs} at {p.grain} grain per {group_label}; additive measures are "
            "summed, peaks take the max and ratios are recomputed from their parts.",
            f"Classification {cls}: the inputs need at least {preview['minimum_classification']} "
            f"(because of {preview['because_of']}).",
        ]
        asked = asked_classification(fields)
        if asked and CLASSIFICATIONS.index(asked) < CLASSIFICATIONS.index(
            preview["minimum_classification"]
        ):
            rationale.append(
                f"The request asked for {asked}, which its inputs do not allow; please confirm "
                f"{cls} works for {', '.join(who)}."
            )
        rationale += p.notes
        name = fields.get("name") or p.name
        return {
            "name": name,
            "description": (
                f"Data product - {_PERIOD[p.grain][1]} {p.topic} per {group_label}, rolled up "
                f"from {inputs}, with how many days each period covers. Requested for "
                f"{', '.join(who)}."
            ),
            "classification": cls,
            "consumers": who,
            "grain": p.grain,
            "primary_key": [p.period, p.group_columns()[0][0]],
            "columns": p.columns(),
            "sql": p.sql(),
            "quality": p.rules,
            "checks": [{"name": k, "sql": v} for k, v in p.checks(name).items()],
            "rationale": " ".join(rationale),
        }

    def _summary(self, fields: dict[str, str], history: list) -> str:
        for name, _args, result, err in history:
            if err:
                return (
                    f"I could not draft this product: {name} said {result['error']}. "
                    "Nothing was saved; adjust the request or run with --llm bedrock."
                )
        p = plan(fields.get("request", ""))
        if p is None:
            return (
                "The offline designer drafts period roll-ups of energy (demand, peak, generation, "
                "emissions), renewable share or frequency excursions per balancing authority, "
                "region or interconnection. This request is outside that; run it with --llm "
                "bedrock to have Claude design it. Nothing was drafted."
            )
        drafted = self._last(history, "draft_data_product")
        if drafted is None:
            return (
                f"I could not draft this: {p.source} is not a table you are cleared to read. "
                "Ask its owner for access, or narrow the request. Nothing was drafted."
            )
        args, result, _ = drafted
        notes = list(p.notes)
        asked = asked_classification(fields)
        if asked and asked != args["classification"]:
            notes.append(
                f"You asked for {asked}, but the inputs make it {args['classification']}; "
                "the reviewer should confirm that suits the consumers"
            )
        tail = (" " + ". ".join(n[0].upper() + n[1:] for n in notes) + ".") if notes else ""
        return (
            f"I drafted {args['name']} ({args['classification']}): {result['rows']} rows, "
            f"{result['periods'][0]} to {result['periods'][1]}, from "
            f"{', '.join(self._last(history, 'preview_transform')[1]['inputs'])}, with "
            f"{len(result['checks_passed'])} checks passing.{tail} "
            f"Review draft {result['draft_id']} with `gridlake products show {result['draft_id']}`, "
            "then approve it to open the pull request."
        )
