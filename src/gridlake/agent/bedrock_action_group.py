"""Amazon Bedrock Agents action group backed by the same access-aware tools.

A managed Bedrock Agent (``infra/terraform/agent.tf``) plans with the model and calls this
Lambda for each tool use. The caller's clearance comes from the session attributes that the
calling application sets on ``InvokeAgent`` from the signed-in user's groups. The model cannot
set or change it, and it defaults to ``public`` when missing.

``python -m gridlake.agent.bedrock_action_group`` prints the function schema that Terraform
reads (``infra/terraform/agent_functions.json``); a test keeps the two in sync.
"""

from __future__ import annotations

import json
import sys
from functools import cache
from typing import Any

from gridlake.agent.tools import SPECS, Toolbox
from gridlake.contracts import CLASSIFICATIONS

_TYPES = {"string": "string", "integer": "integer", "number": "number", "boolean": "boolean"}


def function_schema() -> list[dict[str, Any]]:
    """Tool specs in Bedrock Agents function-details form."""
    out = []
    for spec in SPECS:
        props = spec["input_schema"].get("properties", {})
        required = set(spec["input_schema"].get("required", []))
        out.append(
            {
                "name": spec["name"],
                "description": spec["description"],
                "parameters": {
                    name: {
                        "type": _TYPES[p["type"]],
                        "description": p.get("description")
                        or (f"one of {', '.join(p['enum'])}" if "enum" in p else name),
                        "required": name in required,
                    }
                    for name, p in props.items()
                },
            }
        )
    return out


@cache
def _toolbox(clearance: str) -> Toolbox:
    from gridlake.agent import build_toolbox
    from gridlake.config import Settings

    return build_toolbox(Settings(), clearance)


def _value(param: dict[str, Any]) -> Any:
    return int(param["value"]) if param.get("type") == "integer" else param["value"]


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    attrs = event.get("sessionAttributes") or {}
    clearance = attrs.get("clearance", "public")
    if clearance not in CLASSIFICATIONS:
        clearance = "public"
    args = {p["name"]: _value(p) for p in event.get("parameters") or []}
    result = _toolbox(clearance).call(event["function"], args)
    body = {
        "result": result.content,
        "cite_tables": result.tables,
        "cite_documents": result.documents,
    }
    return {
        "messageVersion": "1.0",
        "response": {
            "actionGroup": event["actionGroup"],
            "function": event["function"],
            "functionResponse": {"responseBody": {"TEXT": {"body": json.dumps(body, default=str)}}},
        },
        "sessionAttributes": attrs,
        "promptSessionAttributes": event.get("promptSessionAttributes") or {},
    }


if __name__ == "__main__":
    json.dump(function_schema(), sys.stdout, indent=2)
    sys.stdout.write("\n")
