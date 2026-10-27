"""The data reliability agent: triages an incident and drafts a fix or issue for approval."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from gridlake.agent.core import Step, run_loop
from gridlake.agent.llm import LLM
from gridlake.reliability.drafts import Draft
from gridlake.reliability.runlog import Incident
from gridlake.reliability.tools import ReliabilityToolbox

INSTRUCTIONS = Path(__file__).with_name("instructions.txt").read_text().strip()


def incident_prompt(incident: Incident) -> str:
    return (
        "A data reliability incident needs triage.\n\n"
        f"```json\n{json.dumps(incident.to_dict(), indent=2)}\n```\n\n"
        "Investigate it with your tools, record a diagnosis, then draft one fix or issue "
        "for a person to approve."
    )


@dataclass
class Triage:
    incident: Incident
    model: str
    clearance: str
    summary: str
    diagnosis: dict[str, Any] | None
    drafts: list[Draft] = field(default_factory=list)
    steps: list[Step] = field(default_factory=list)
    tables: list[str] = field(default_factory=list)
    stop_reason: str = "end_turn"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ReliabilityAgent:
    def __init__(self, llm: LLM, max_steps: int = 16):
        self.llm, self.max_steps = llm, max_steps

    def triage(self, incident: Incident, toolbox: ReliabilityToolbox) -> Triage:
        t = run_loop(self.llm, INSTRUCTIONS, toolbox, incident_prompt(incident), self.max_steps)
        return Triage(
            incident,
            self.llm.name,
            toolbox.clearance,
            t.text,
            toolbox.diagnosis,
            list(toolbox.created),
            t.steps,
            t.tables,
            t.stop_reason,
        )
