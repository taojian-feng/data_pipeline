"""The self-service data product agent: turns a described dataset into a drafted product."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from gridlake.agent.core import Step, run_loop
from gridlake.agent.llm import LLM
from gridlake.reliability.drafts import Draft
from gridlake.selfservice.tools import ProductToolbox

INSTRUCTIONS = Path(__file__).with_name("instructions.txt").read_text().strip()


@dataclass
class Request:
    text: str
    owner: str
    name: str | None = None
    consumers: list[str] = field(default_factory=list)
    classification: str | None = None

    def prompt(self) -> str:
        lines = [
            "A data consumer has asked for a new data product.",
            "",
            f"Request: {' '.join(self.text.split())}",
            f"Requested by: {self.owner}",
        ]
        if self.name:
            lines.append(f"Name: {self.name}")
        if self.consumers:
            lines.append(f"Consumers: {', '.join(self.consumers)}")
        if self.classification:
            lines.append(f"Classification asked for: {self.classification}")
        lines += [
            "",
            "Explore the governed tables, preview the transformation, then draft the contract, "
            "SQL and tests for a person to approve.",
        ]
        return "\n".join(lines)


@dataclass
class Design:
    request: Request
    model: str
    clearance: str
    summary: str
    drafts: list[Draft] = field(default_factory=list)
    steps: list[Step] = field(default_factory=list)
    tables: list[str] = field(default_factory=list)
    stop_reason: str = "end_turn"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ProductDesigner:
    def __init__(self, llm: LLM, max_steps: int = 14):
        self.llm, self.max_steps = llm, max_steps

    def design(self, request: Request, toolbox: ProductToolbox) -> Design:
        t = run_loop(self.llm, INSTRUCTIONS, toolbox, request.prompt(), self.max_steps)
        return Design(
            request,
            self.llm.name,
            toolbox.clearance,
            t.text,
            list(toolbox.created),
            t.steps,
            t.tables,
            t.stop_reason,
        )
