"""Models that drive the grid analyst: Claude on Amazon Bedrock, or an offline planner.

Both speak the Anthropic Messages shape (``tool_use`` / ``tool_result`` content blocks), so
the agent loop in ``gridlake.agent.core`` is the same with or without a cloud account.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Protocol

_DATE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")


@dataclass
class Reply:
    stop_reason: str
    content: list[Any]  # content blocks: SDK objects from Claude, dicts from the planner


class LLM(Protocol):
    name: str

    def respond(
        self, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> Reply: ...


def block_get(block: Any, key: str, default: Any = None) -> Any:
    return block.get(key, default) if isinstance(block, dict) else getattr(block, key, default)


class BedrockClaude:
    """Claude on Amazon Bedrock through the Anthropic SDK's Bedrock (Mantle) client, which
    signs requests with the standard AWS credential chain. Needs the ``aws`` extra."""

    def __init__(
        self, model: str, region: str, client: Any = None, max_tokens: int = 16000
    ) -> None:
        if client is None:
            from anthropic import AnthropicBedrockMantle  # optional dependency

            client = AnthropicBedrockMantle(aws_region=region)
        self.client, self.model, self.max_tokens = client, model, max_tokens
        self.name = f"bedrock:{model}"

    def respond(
        self, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> Reply:
        response = self.client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            system=system,
            tools=tools,
            messages=messages,
        )
        # Keep the blocks as returned (thinking blocks must go back unchanged).
        return Reply(response.stop_reason, list(response.content))


# -- offline planner ----------------------------------------------------------

# (keyword pattern, table, column, label, unit, lower_is_better)
_METRICS = (
    (r"renewable", "gold.ba_daily_energy", "renewable_share_pct", "renewable share", "%", False),
    (
        r"carbon|co2 intensity|cleanest|dirtiest",
        "gold.ba_daily_energy",
        "carbon_intensity_kg_per_mwh",
        "carbon intensity",
        " kg CO2/MWh",
        True,
    ),
    (r"emission", "gold.ba_daily_energy", "total_emissions_t", "emissions", " t CO2", True),
    (
        r"forecast|mape|accura",
        "gold.ba_daily_energy",
        "forecast_mape_pct",
        "day-ahead forecast error (MAPE)",
        "%",
        True,
    ),
    (
        r"frequency|excursion|reliab",
        "gold.grid_reliability_daily",
        "excursion_minutes",
        "frequency excursion minutes",
        " min",
        True,
    ),
    (r"peak", "gold.ba_daily_energy", "peak_demand_mwh", "peak demand", " MWh", False),
    (
        r"demand|load|consumption",
        "gold.ba_daily_energy",
        "total_demand_mwh",
        "daily demand",
        " MWh",
        False,
    ),
)
_CATALOG = re.compile(
    r"which table|what table|who owns|\bowner|lineage|derived|upstream|data product"
)
_RANKING = re.compile(
    r"highest|lowest|most|least|best|worst|top|rank|which (balancing|ba)|compare|cleanest|dirtiest|largest|smallest"
)
_ASCENDING = re.compile(r"lowest|least|smallest|fewest|cleanest|minimum")


def _fmt(v: Any, unit: str) -> str:
    if isinstance(v, int | float):
        return f"{v:,.1f}{unit}" if abs(v) < 1000 else f"{v:,.0f}{unit}"
    return f"{v}{unit}"


class OfflinePlanner:
    """A deterministic, rule-based stand-in for the model, so ``gridlake ask`` and the tests
    run with no cloud account. It is not an LLM: it covers metric lookups, rankings and
    catalog questions by choosing tools the same way the prompt asks Claude to (resolve
    entities, query gold with SQL, describe the backing table) and writes a templated answer
    from the tool results. Anything else falls back to document search."""

    name = "offline-planner"

    def respond(
        self, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> Reply:
        question = messages[0]["content"]
        history = self._history(messages)
        call = self._next_call(question, history)
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
        return Reply("end_turn", [{"type": "text", "text": self._answer(question, history)}])

    @staticmethod
    def _history(messages: list[dict[str, Any]]) -> list[tuple[str, dict, Any, bool]]:
        calls: dict[str, tuple[str, dict]] = {}
        out = []
        for m in messages[1:]:
            if not isinstance(m["content"], list):
                continue
            for b in m["content"]:
                if block_get(b, "type") == "tool_use":
                    calls[block_get(b, "id")] = (block_get(b, "name"), block_get(b, "input"))
                elif block_get(b, "type") == "tool_result":
                    name, args = calls[b["tool_use_id"]]
                    out.append((name, args, json.loads(b["content"]), bool(b.get("is_error"))))
        return out

    @staticmethod
    def _metric(q: str) -> tuple | None:
        return next((m for m in _METRICS if re.search(m[0], q)), None)

    def _next_call(self, question: str, history: list) -> tuple[str, dict] | None:
        q = question.lower()
        done = [h[0] for h in history]
        metric = self._metric(q)
        if metric is None and _CATALOG.search(q):
            if not done:
                return "search_documents", {"query": question, "doc_type": "catalog", "k": 2}
            passages = history[0][2].get("passages", []) if not history[0][3] else []
            if "describe_table" not in done and passages and passages[0].get("source_table"):
                return "describe_table", {"table": passages[0]["source_table"]}
            return None
        if metric is None:
            return ("search_documents", {"query": question, "k": 4}) if not done else None
        if not done:
            return "lookup_entity", {"text": question}
        if "run_sql" not in done:
            return "run_sql", {"sql": self._sql(q, metric, self._ba_codes(history))}
        if _CATALOG.search(q) and "describe_table" not in done:
            return "describe_table", {"table": metric[1]}
        return None

    @staticmethod
    def _ba_codes(history: list) -> list[str]:
        for name, _, result, err in history:
            if name == "lookup_entity" and not err:
                return [
                    e["code"]
                    for e in result["entities"]
                    if e["type"] == "BalancingAuthority" and "code" in e
                ]
        return []

    @staticmethod
    def _sql(q: str, metric: tuple, codes: list[str]) -> str:
        _, table, col, *_ = metric
        days = sorted(set(_DATE.findall(q)))
        where = []
        if codes and not (_RANKING.search(q) and len(codes) != 1):
            where.append("ba_code IN (" + ", ".join(f"'{c}'" for c in codes) + ")")
        if len(days) == 1:
            where.append(f"date = DATE '{days[0]}'")
        elif len(days) == 2:
            where.append(f"date BETWEEN DATE '{days[0]}' AND DATE '{days[1]}'")
        clause = f" WHERE {' AND '.join(where)}" if where else ""
        if len(days) == 1 and any(w.startswith("ba_code") for w in where):
            return f"SELECT date, ba_code, {col} AS value FROM {table}{clause} ORDER BY ba_code"
        order = (
            "ASC"
            if _ASCENDING.search(q) or (metric[5] and re.search(r"best|most accurate", q))
            else "DESC"
        )
        return (
            f"SELECT ba_code, min(date) AS first_day, max(date) AS last_day, "
            f"round(avg({col}), 2) AS value FROM {table}{clause} "
            f"GROUP BY ba_code ORDER BY value {order} LIMIT 10"
        )

    def _answer(self, question: str, history: list) -> str:
        q = question.lower()
        metric = self._metric(q)
        parts: list[str] = []
        for name, args, result, err in history:
            if err:
                if name in ("run_sql", "describe_table", "search_documents"):
                    parts.append(f"I can't answer that with your access: {result['error']}.")
                continue
            if name == "run_sql" and metric:
                parts.append(self._sql_sentence(result, metric))
            elif name == "describe_table":
                up = ", ".join(result["derived_from"]) or "no recorded upstream"
                parts.append(
                    f"The governed table is {result['table']} ({result['classification']}, owned by "
                    f"{result['owner']}, contract v{result['contract_version']}), derived from {up}."
                )
            elif name == "search_documents" and "describe_table" not in [h[0] for h in history]:
                passages = result["passages"]
                if passages:
                    parts.append(" ".join(f"[{p['doc_id']}] {p['text']}" for p in passages[:2]))
                elif args.get("doc_type") == "catalog":
                    parts.append(
                        "I found no catalog entries you are cleared to see. Catalog metadata "
                        "(owners, lineage, rules) needs at least internal clearance."
                    )
                else:
                    parts.append("I found no passages you are cleared to see for that question.")
        return " ".join(parts) or "I could not find data to answer that question."

    @staticmethod
    def _sql_sentence(result: dict, metric: tuple) -> str:
        _, table, _, label, unit, _ = metric
        cols, rows = (
            result["columns"],
            [dict(zip(result["columns"], r, strict=True)) for r in result["rows"]],
        )
        if not rows:
            return f"No rows in {table} matched that question."
        if "date" in cols:
            return " ".join(
                f"{r['ba_code']} {label} on {r['date']}: {_fmt(r['value'], unit)}." for r in rows
            )
        window = f"{min(r['first_day'] for r in rows)} to {max(r['last_day'] for r in rows)}"
        if len(rows) == 1:
            return f"{rows[0]['ba_code']} averaged {_fmt(rows[0]['value'], unit)} {label} from {window}."
        listing = "; ".join(
            f"{i}. {r['ba_code']} {_fmt(r['value'], unit)}" for i, r in enumerate(rows, 1)
        )
        return (
            f"{rows[0]['ba_code']} ranks first by average {label} ({_fmt(rows[0]['value'], unit)}, "
            f"{window}). Ranking: {listing}."
        )
