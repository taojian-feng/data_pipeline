"""The grid anomaly watcher: explains what detection flagged and writes the daily brief.

Detection (``watcher.detect``) decides *what* is unusual; the model only explains it, with
the shared tool loop and the watcher's access-aware tools. The brief's table of numbers is
rendered from the detection itself, never from model output, so a model can change the
wording of a brief but not its figures.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from gridlake.agent.core import Step, run_loop
from gridlake.agent.llm import LLM
from gridlake.watcher.detect import SIGNALS, Detection
from gridlake.watcher.tools import WatcherToolbox

INSTRUCTIONS = Path(__file__).with_name("instructions.txt").read_text().strip()


def brief_prompt(detection: Detection) -> str:
    return (
        f"The detector flagged these changes on {detection.date} across "
        f"{len(detection.balancing_authorities)} balancing authorities. Explain them for "
        "today's grid brief.\n\n"
        f"```json\n{json.dumps([a.to_dict() for a in detection.anomalies], indent=2)}\n```"
    )


@dataclass
class Brief:
    date: str
    model: str
    clearance: str
    narrative: str
    detection: Detection
    steps: list[Step] = field(default_factory=list)
    tables: list[str] = field(default_factory=list)
    stop_reason: str = "end_turn"

    @property
    def title(self) -> str:
        n = len(self.detection.anomalies)
        found = f"{n} unusual change{'s' * (n != 1)}" if n else "no unusual changes"
        return f"Grid brief for {self.date}: {found}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date,
            "title": self.title,
            "model": self.model,
            "clearance": self.clearance,
            "narrative": self.narrative,
            "detection": self.detection.to_dict(),
            "steps": [s.__dict__ for s in self.steps],
            "tables": self.tables,
            "stop_reason": self.stop_reason,
        }

    def markdown(self) -> str:
        d = self.detection
        lines = [f"# {self.title}", "", self.narrative, ""]
        if d.anomalies:
            lines += [
                "## What changed",
                "",
                "| Severity | BA | Signal | Today | Compared with | Change | z |",
                "|---|---|---|---:|---|---:|---:|",
            ]
            for a in d.anomalies:
                if a.window.method == "previous_day":
                    against = f"{a.baseline:,.1f} the day before"
                elif a.signal == "new_source":
                    against = "no history"
                else:
                    kind = a.window.day_type if a.window.day_type != "all" else "day"
                    against = f"{a.baseline:,.1f} (median {kind}, {a.window.days} days)"
                z = "" if a.z is None else f"{a.z:+.1f}"
                lines.append(
                    f"| {a.severity} | {a.ba_code} | {a.label()} | {a.value:,.1f} | {against} "
                    f"| {a.change:+.1f}{a.unit} | {z} |"
                )
            lines.append("")
        minimums = ", ".join(f"{s.replace('_', ' ')} {m:g}{u}" for s, (m, _, u) in SIGNALS.items())
        notes = [
            f"Checked {len(d.balancing_authorities)} balancing authorities against up to "
            f"{d.lookback_days} earlier days. A change is flagged when its robust z-score is at "
            f"least 3.5 and it moves at least the minimum ({minimums}; fuel shares as a one-day "
            "move). Demand is compared per reported hour with the same day type."
        ]
        if d.data_gaps:
            gaps = ", ".join(f"{ba} {h}/24h" for ba, h in sorted(d.data_gaps.items()))
            notes.append(f"Hours reported short of 24: {gaps}.")
        if d.not_judged:
            notes.append(
                "Not enough history to judge: "
                + "; ".join(
                    f"{k.replace('_', ' ')} for {', '.join(v)}" for k, v in d.not_judged.items()
                )
                + "."
            )
        lines += ["## Notes", ""] + [f"- {n}" for n in notes] + [""]
        tables = ", ".join(self.tables or ["gold.ba_daily_energy", "gold.fuel_mix_daily"])
        by = f"Written by {self.model}, " if self.model != "none" else ""
        lines.append(f"_Sources: {tables}. {by}clearance {self.clearance}._")
        return "\n".join(lines) + "\n"


class AnomalyWatcher:
    def __init__(self, llm: LLM, max_steps: int = 8):
        self.llm, self.max_steps = llm, max_steps

    def brief(self, detection: Detection, toolbox: WatcherToolbox) -> Brief:
        if not detection.anomalies:
            n = len(detection.balancing_authorities)
            text = (
                f"No unusual demand, interchange or fuel-mix changes across {n} balancing "
                f"authorities on {detection.date}."
                if n
                else f"No gold data for {detection.date} yet."
            )
            return Brief(detection.date, "none", toolbox.clearance, text, detection)
        t = run_loop(self.llm, INSTRUCTIONS, toolbox, brief_prompt(detection), self.max_steps)
        tables = sorted(set(t.tables) | {x for a in detection.anomalies for x in a.tables})
        return Brief(
            detection.date,
            self.llm.name,
            toolbox.clearance,
            t.text,
            detection,
            t.steps,
            tables,
            t.stop_reason,
        )
