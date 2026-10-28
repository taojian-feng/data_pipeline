"""Tools for the grid anomaly watcher, built on the grid analyst's access-aware toolbox.

* ``get_anomaly_context``  everything else that moved for one detected anomaly: the BA's day
                           against its usual one, fuel moves since the day before, peers in
                           the same interconnection, and plain-language cues.
* ``run_sql`` / ``lookup_entity`` / ``describe_table`` / ``list_tables`` from the analyst
  toolbox, for anything the context does not cover.

The watcher's clearance is bound outside the model, as for the other agents. The brief goes to
a wide audience, so it runs with ``public`` clearance unless configured otherwise.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from gridlake.agent.tools import SPECS as ANALYST_SPECS
from gridlake.agent.tools import SqlEngine, Toolbox, ToolResult
from gridlake.contracts import Contract
from gridlake.knowledge.graph import KnowledgeGraph
from gridlake.watcher.context import TABLES, WatchData

_KEEP = {"list_tables", "run_sql", "describe_table", "lookup_entity"}

SPECS: list[dict[str, Any]] = [
    {
        "name": "get_anomaly_context",
        "description": (
            "Evidence for one detected anomaly, by its id: the balancing authority's whole day "
            "against its usual one (demand, peak, day-ahead forecast error, net interchange and "
            "whether it exported or imported, generation, renewable share, carbon intensity), "
            "the fuels whose share moved most since the day before, how peers in the same "
            "interconnection moved, related anomalies, and cues read off that evidence."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"anomaly_id": {"type": "string", "description": "id from the prompt"}},
            "required": ["anomaly_id"],
            "additionalProperties": False,
        },
    },
    *[s for s in ANALYST_SPECS if s["name"] in _KEEP],
]


class WatcherToolbox(Toolbox):
    def __init__(
        self,
        contracts: dict[str, Contract],
        clearance: str,
        sql: SqlEngine,
        data: WatchData,
        graph: Callable[[], KnowledgeGraph] | None = None,
        max_rows: int = 50,
    ):
        super().__init__(contracts, clearance, sql, graph=graph, max_rows=max_rows)
        self.data = data

    def specs(self) -> list[dict[str, Any]]:
        return SPECS

    def _tool_get_anomaly_context(self, anomaly_id: str) -> ToolResult:
        for table in TABLES:
            self._check_rows(self._contract(table))
        return ToolResult(self.data.context(anomaly_id), tables=list(TABLES))
