"""The grid analyst: a tool-using agent loop over access-aware tools.

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


class GridAnalyst:
    def __init__(self, llm: LLM, toolbox: Toolbox, max_steps: int = 8):
        self.llm, self.toolbox, self.max_steps = llm, toolbox, max_steps

    def ask(self, question: str) -> Answer:
        answer = Answer(question, self.toolbox.clearance, self.llm.name, text="")
        messages: list[dict[str, Any]] = [{"role": "user", "content": question}]
        for _ in range(self.max_steps):
            reply = self.llm.respond(INSTRUCTIONS, messages, self.toolbox.specs())
            messages.append({"role": "assistant", "content": reply.content})
            calls = [b for b in reply.content if block_get(b, "type") == "tool_use"]
            answer.stop_reason = reply.stop_reason
            if reply.stop_reason != "tool_use" or not calls:
                answer.text = "".join(
                    block_get(b, "text", "")
                    for b in reply.content
                    if block_get(b, "type") == "text"
                ).strip()
                if reply.stop_reason == "refusal" and not answer.text:
                    answer.text = "The model declined to answer this question."
                break
            results = []
            for call in calls:
                name, args = block_get(call, "name"), block_get(call, "input") or {}
                result = self.toolbox.call(name, args)
                answer.steps.append(
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
                    answer.tables += [t for t in result.tables if t not in answer.tables]
                    answer.documents += [d for d in result.documents if d not in answer.documents]
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
            answer.stop_reason = "max_steps"
            answer.text = f"Stopped after {self.max_steps} tool rounds without a final answer."
        return answer
