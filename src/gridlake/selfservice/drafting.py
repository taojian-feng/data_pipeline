"""Rendering a data product draft: the contract YAML, the SQL file, the tests and the patch.

The model chooses the content; these functions choose the form, so every drafted contract
reads like the hand-written ones in ``contracts/`` and every test file has the same shape.
"""

from __future__ import annotations

import difflib
import json
import re
import textwrap
from typing import Any

_PLAIN = re.compile(r"^[A-Za-z_][A-Za-z0-9_ .@/()%+-]*$")
_RESERVED = {"true", "false", "yes", "no", "on", "off", "null", "y", "n", "~"}


def scalar(v: Any) -> str:
    """A YAML scalar for a flow mapping: plain when safe, else a JSON (double-quoted) string."""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int | float):
        return repr(v)
    s = str(v)
    if _PLAIN.match(s) and s.strip() == s and s.lower() not in _RESERVED and ": " not in s:
        return s
    return json.dumps(s)


def flow(d: dict[str, Any]) -> str:
    parts = []
    for k, v in d.items():
        if isinstance(v, list):
            parts.append(f"{k}: [{', '.join(scalar(x) for x in v)}]")
        else:
            parts.append(f"{k}: {scalar(v)}")
    return "{" + ", ".join(parts) + "}"


def render_contract(c: dict[str, Any]) -> str:
    """Contract YAML in the house style: block keys, one flow mapping per column and rule."""
    lines = [
        f"name: {c['name']}",
        f"version: {c['version']}",
        f"layer: {c['layer']}",
        f"domain: {scalar(c['domain'])}",
        f"owner: {scalar(c['owner'])}",
        f"classification: {c['classification']}",
        "description: >",
        *textwrap.wrap(
            " ".join(c["description"].split()),
            width=94,
            initial_indent="  ",
            subsequent_indent="  ",
            break_on_hyphens=False,
            break_long_words=False,
        ),
        f"primary_key: [{', '.join(c['primary_key'])}]",
        "partition_by:",
        *(f"  - {flow(p)}" for p in c["partition_by"]),
        f"consumers: [{', '.join(scalar(x) for x in c['consumers'])}]",
        "transform:",
        f"  sql: {c['transform']['sql']}",
        f"  inputs: [{', '.join(c['transform']['inputs'])}]",
        f"  grain: {c['transform']['grain']}",
        "columns:",
        *(f"  - {flow(col)}" for col in c["columns"]),
        "quality:",
        *(f"  - {flow(r)}" for r in c["quality"]),
    ]
    return "\n".join(lines) + "\n"


def render_sql(table: str, request: str, sql: str) -> str:
    head = f"-- {table}: drafted by the gridlake self-service data product agent.\n"
    head += "".join(f"-- {line}\n" for line in textwrap.wrap(f"Request: {request}", 96))
    head += "-- Reads only the inputs its contract declares; the pipeline builds it per period.\n"
    return head + textwrap.dedent(sql).strip() + "\n"


def test_module_name(table: str) -> str:
    return "test_" + table.replace(".", "_")


def render_test(
    table: str, request: str, owner: str, columns: list[str], checks: dict[str, str]
) -> str:
    request_lines = textwrap.wrap(request, 88) or [""]
    check_items = "".join(
        f'    {json.dumps(name)}: """\n'
        + textwrap.indent(textwrap.dedent(sql).strip(), "        ")
        + '\n    """,\n'
        for name, sql in sorted(checks.items())
    )
    return (
        f'"""Tests for {table}, drafted by the gridlake self-service data product agent.\n\n'
        "Request:\n"
        + "".join(f"    {line}\n" for line in request_lines)
        + f"\nOwned by {owner} once merged; edit them like any other test.\n"
        '"""\n\n'
        "from pathlib import Path\n\n"
        "import pytest\n\n"
        "from gridlake.transform.declared import verify_product\n\n"
        "REPO = Path(__file__).resolve().parents[2]\n"
        f"TABLE = {json.dumps(table)}\n"
        "COLUMNS = [\n" + "".join(f"    {json.dumps(c)},\n" for c in columns) + "]\n"
        "# Each check is a SELECT that returns the rows violating it; no rows means it passes.\n"
        "CHECKS = {\n" + check_items + "}\n\n\n"
        '@pytest.fixture(scope="module")\n'
        "def product(tmp_path_factory):\n"
        '    return verify_product(TABLE, tmp_path_factory.mktemp("product"), REPO, CHECKS)\n\n\n'
        "def test_meets_the_governance_rules(product):\n"
        "    assert product.governance == []\n\n\n"
        "def test_builds_rows_with_the_contract_columns(product):\n"
        "    assert product.rows > 0\n"
        "    assert product.columns == COLUMNS\n\n\n"
        "def test_passes_its_quality_rules(product):\n"
        "    assert product.quality_failures == []\n\n\n"
        '@pytest.mark.parametrize("check", sorted(CHECKS))\n'
        "def test_check(product, check):\n"
        "    assert product.check_failures[check] == 0\n\n\n"
        "def test_rebuilding_every_period_changes_nothing(product):\n"
        "    assert product.idempotent\n"
    )


def new_files_patch(files: dict[str, str]) -> str:
    """One git patch that creates every file, with repo-relative paths."""
    out = []
    for path, text in files.items():
        lines = text.splitlines(keepends=True)
        out.append(f"diff --git a/{path} b/{path}\nnew file mode 100644\n")
        out += difflib.unified_diff([], lines, fromfile="/dev/null", tofile=f"b/{path}")
    return "".join(out)


def markdown_table(columns: list[str], rows: list[list[Any]]) -> str:
    def cell(v: Any) -> str:
        return ("" if v is None else str(v)).replace("|", "\\|")

    head = "| " + " | ".join(columns) + " |\n|" + "---|" * len(columns) + "\n"
    return head + "".join("| " + " | ".join(cell(v) for v in r) + " |\n" for r in rows)
