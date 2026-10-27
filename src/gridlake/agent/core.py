"""The tool-using agent loop, and the grid analyst built on it.

The model chooses which tools to call and in what order; the loop runs them with the
caller's clearance, feeds results back, and collects the tables and documents every
successful tool call touched, so each answer carries its citations.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from gridlake.agent.llm import LLM, block_get
from gridlake.agent.tools import Toolbox

INSTRUCTIONS = Path(__file__).with_name("instructions.txt").read_text().strip()


@dataclass
class Step:
    tool: str
    input: dict[str, Any]
    ok: bool
    tables: list[str]
    documents: list[str]
    error: str | None = None


@dataclass
class Answer:
    question: str
    clearance: str
    model: str
    text: str
    tables: list[str] = field(default_factory=list)
    documents: list[str] = field(default_factory=list)
    steps: list[Step] = field(default_factory=list)
    stop_reason: str = "end_turn"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Transcript:
    text: str = ""
    steps: list[Step] = field(default_factory=list)
    tables: list[str] = field(default_factory=list)
    documents: list[str] = field(default_factory=list)
    stop_reason: str = "end_turn"


def run_loop(
    llm: LLM, system: str, toolbox: Toolbox, prompt: str, max_steps: int = 8
) -> Transcript:
    """The tool loop every gridlake agent shares: the model asks for tools, the toolbox runs
    them under the caller's access, results go back, until the model answers or the step
    budget runs out. Tables and documents from successful calls become the citations."""
    out = Transcript()
    messages: list[dict[str, Any]] = [{"role": "user", "content": prompt}]
    for _ in range(max_steps):
        reply = llm.respond(system, messages, toolbox.specs())
        messages.append({"role": "assistant", "content": reply.content})
        calls = [b for b in reply.content if block_get(b, "type") == "tool_use"]
        out.stop_reason = reply.stop_reason
        if reply.stop_reason != "tool_use" or not calls:
            out.text = "".join(
                block_get(b, "text", "") for b in reply.content if block_get(b, "type") == "text"
            ).strip()
            if reply.stop_reason == "refusal" and not out.text:
                out.text = "The model declined to answer this question."
            break
        results = []
        for call in calls:
            name, args = block_get(call, "name"), block_get(call, "input") or {}
            result = toolbox.call(name, args)
            out.steps.append(
                Step(
                    name,
                    args,
                    not result.is_error,
                    result.tables,
                    result.documents,
                    result.content.get("error") if result.is_error else None,
                )
            )
            if not result.is_error:
                out.tables += [t for t in result.tables if t not in out.tables]
                out.documents += [d for d in result.documents if d not in out.documents]
            results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": block_get(call, "id"),
                    "content": json.dumps(result.content, default=str),
                    "is_error": result.is_error,
                }
            )
        # All results for one turn go back in a single user message.
        messages.append({"role": "user", "content": results})
    else:
        out.stop_reason = "max_steps"
        out.text = f"Stopped after {max_steps} tool rounds without a final answer."
    return out


class GridAnalyst:
    def __init__(self, llm: LLM, toolbox: Toolbox, max_steps: int = 8):
        self.llm, self.toolbox, self.max_steps = llm, toolbox, max_steps

    def ask(self, question: str) -> Answer:
        t = run_loop(self.llm, INSTRUCTIONS, self.toolbox, question, self.max_steps)
        return Answer(
            question,
            self.toolbox.clearance,
            self.llm.name,
            t.text,
            t.tables,
            t.documents,
            t.steps,
            t.stop_reason,
        )
