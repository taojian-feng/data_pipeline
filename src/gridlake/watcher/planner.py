"""A deterministic stand-in for the model that writes the anomaly brief.

Like the other offline planners it is not an LLM. It works the way the instructions ask
Claude to: it asks for the context of every detected anomaly in one turn, then writes one
paragraph per balancing authority from the numbers and cues the context returned.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from typing import Any

from gridlake.agent.llm import OfflinePlanner, Reply
from gridlake.sources.eia import FUEL_NAMES


def anomalies_from_prompt(prompt: str) -> list[dict[str, Any]]:
    m = re.search(r"```json\n(.*?)\n```", prompt, re.S)
    return json.loads(m.group(1)) if m else []


def _sentence(text: str) -> str:
    text = text.strip()
    return text[:1].upper() + text[1:] + ("" if text.endswith(".") else ".")


def describe_fuel(a: dict[str, Any]) -> str:
    return FUEL_NAMES.get(a.get("fuel_type") or "", a.get("fuel_type") or "")


def describe(a: dict[str, Any]) -> str:
    """One clause stating what changed, with the numbers."""
    fuel = FUEL_NAMES.get(a.get("fuel_type") or "", a.get("fuel_type"))
    change, value, base = a["change"], a["value"], a["baseline"]
    if a["signal"] == "demand":
        side = "above" if change > 0 else "below"
        day = a["window"]["day_type"]
        usual = f"a usual {day}" if day in ("weekday", "weekend") else "usual"
        return (
            f"demand ran {abs(change):.1f}% {side} {usual} "
            f"({value:,.0f} MWh an hour against {base:,.0f})"
        )
    if a["signal"] == "interchange":
        return (
            f"net interchange moved {change:+.1f} points of demand, "
            f"to {value:+.1f}% (usually {base:+.1f}%)"
        )
    if a["signal"] == "fuel_share":
        verb = "rose" if change > 0 else "fell"
        note = f", the {a['note']}" if a.get("note") else ""
        return (
            f"{fuel} share {verb} {abs(change):.1f} points in a day, "
            f"to {value:.1f}% (from {base:.1f}%){note}"
        )
    return f"a new energy source, {fuel} ({a['fuel_type']}), supplied {value:.1f}% of generation"


class OfflineBriefWriter:
    name = "offline-brief-writer"

    def respond(
        self, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> Reply:
        anomalies = anomalies_from_prompt(messages[0]["content"])
        history = OfflinePlanner._history(messages)
        asked = {args.get("anomaly_id") for name, args, _, _ in history}
        todo = [a["id"] for a in anomalies if a["id"] not in asked]
        if todo and not history:
            return Reply(
                "tool_use",
                [
                    {
                        "type": "tool_use",
                        "id": f"call_{i}",
                        "name": "get_anomaly_context",
                        "input": {"anomaly_id": aid},
                    }
                    for i, aid in enumerate(todo, 1)
                ],
            )
        return Reply("end_turn", [{"type": "text", "text": self._write(anomalies, history)}])

    def _write(self, anomalies: list[dict], history: list) -> str:
        contexts = {args["anomaly_id"]: res for name, args, res, err in history if not err}
        denied = [res["error"] for _, _, res, err in history if err]
        by_ba: dict[str, list[dict]] = defaultdict(list)
        for a in anomalies:  # already most severe first
            by_ba[a["ba_code"]].append(a)
        lead = max(anomalies, key=lambda a: (a["severity"] == "high", abs(a["z"] or 0)))
        count = len(by_ba)
        parts = [
            f"{len(anomalies)} unusual change{'s' * (len(anomalies) != 1)} across "
            f"{count} balancing authorit{'ies' if count != 1 else 'y'} on {lead['date']}; "
            f"the largest is at {lead['ba_code']}, where {describe(lead)}."
        ]
        for ba, items in by_ba.items():
            name = items[0]["ba_name"]
            clauses = [describe(a) for a in items]
            text = f"**{ba}** ({name}): " + "; ".join(clauses) + "."
            # "X was offset by Y" adds nothing when Y is itself one of this BA's anomalies.
            flagged = {describe_fuel(a) for a in items if a.get("fuel_type")}
            seen: list[str] = []
            for a in items:
                for h in (contexts.get(a["id"]) or {}).get("hints", []):
                    offset = " was offset by " in h
                    if offset and any(f in h.split(" was offset by ")[1] for f in flagged):
                        continue
                    if h not in seen:
                        seen.append(h)
            if seen:
                text += " " + " ".join(_sentence(h) for h in seen)
            parts.append(text)
        if denied:
            parts.append(
                "Some evidence was not available with this clearance: " + "; ".join(denied) + "."
            )
        return "\n\n".join(parts)
