"""Access-aware tools for the grid analyst agent.

The caller's clearance is bound when the toolbox is built, from the authenticated session
and never from the model, and every tool enforces it before touching data:

* ``run_sql``          read-only SQL over governed tables. Tables above the caller's clearance
                       are refused before the query runs; on Athena, Lake Formation refuses
                       them again for the agent's role.
* ``list_tables``      the tables the caller may query, with their columns.
* ``describe_table``   owner, lineage, quality rules and consumers. This is catalog metadata,
                       so it is never more open than ``internal`` (ADR 0005).
* ``lookup_entity``    knowledge-graph facts, filtered by clearance.
* ``search_documents`` clearance-filtered retrieval: the local index, or Bedrock Knowledge
                       Base ``Retrieve`` with a classification metadata filter.

Every result names the tables and documents it came from, so answers can cite them.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Protocol

import duckdb

from gridlake.contracts import Contract, cleared, metadata_classification
from gridlake.knowledge.context import link_entities
from gridlake.knowledge.graph import KnowledgeGraph, Literal
from gridlake.knowledge.retrieval import VectorIndex, allowed_classifications

MAX_ROWS = 50
_QUALIFIED = re.compile(r"\b(bronze|silver|gold|quality)\s*\.\s*\"?([a-z_][a-z0-9_]*)", re.I)


class AccessDenied(PermissionError):
    pass


@dataclass
class ToolResult:
    content: Any
    tables: list[str] = field(default_factory=list)
    documents: list[str] = field(default_factory=list)
    is_error: bool = False


# -- SQL ----------------------------------------------------------------------


def referenced_tables(sql: str, strict: bool = True) -> set[str]:
    """Tables read by one read-only statement, as ``namespace.table``.

    Parsing uses DuckDB's own parser, so CTEs, subqueries and joins are all seen. Anything
    but a single SELECT, and any table function (``read_parquet`` and friends), is refused.
    With ``strict=False`` (Athena, whose Trino dialect DuckDB may not parse) a parse failure
    falls back to the qualified names found in the text; Lake Formation is the backstop.
    """
    found = {f"{a.lower()}.{b.lower()}" for a, b in _QUALIFIED.findall(sql)}
    parsed = json.loads(duckdb.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()[0])
    if parsed.get("error"):
        message = parsed.get("error_message", "could not parse")
        if strict or "Only SELECT" in message:
            raise ValueError(f"only a single read-only SELECT is allowed ({message})")
        return found
    if len(parsed["statements"]) != 1:
        raise ValueError("send exactly one SELECT statement")
    ctes: set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for entry in (node.get("cte_map") or {}).get("map", []):
                ctes.add(entry["key"].lower())
            if node.get("type") == "TABLE_FUNCTION":
                raise ValueError("table functions are not allowed; query governed tables")
            if node.get("type") == "BASE_TABLE":
                schema, table = node.get("schema_name", ""), node["table_name"].lower()
                if schema:
                    found.add(f"{schema.lower()}.{table}")
                elif table not in ctes:
                    found.add(table)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(parsed["statements"])
    return found


def _jsonable(v: Any) -> Any:
    if isinstance(v, float):
        return round(v, 3)
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, date | datetime):
        return v.isoformat()
    return v


class SqlEngine(Protocol):
    strict: bool

    def run(self, sql: str, tables: set[str], limit: int) -> tuple[list[str], list[list]]: ...


class LocalSql:
    """DuckDB over Arrow scans of the Iceberg tables. Only the tables the statement names
    (already access-checked) are attached, and file access is switched off and locked."""

    strict = True

    def __init__(self, read: Callable[[str], Any]):
        self.read = read  # e.g. Lakehouse.read

    def run(self, sql: str, tables: set[str], limit: int) -> tuple[list[str], list[list]]:
        con = duckdb.connect()
        con.execute("SET TimeZone = 'UTC'")
        for name in sorted(tables):
            ns, table = name.split(".")
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {ns}")
            con.register(f"_{ns}_{table}", self.read(name))
            con.execute(f'CREATE VIEW {ns}.{table} AS SELECT * FROM "_{ns}_{table}"')
        con.execute("SET enable_external_access = false")
        con.execute("SET lock_configuration = true")
        rel = con.sql(sql)
        rows = rel.limit(limit + 1).fetchall()
        return list(rel.columns), [[_jsonable(v) for v in r] for r in rows]


class AthenaSql:
    """Amazon Athena over the Glue Data Catalog (databases are named after the layers, so
    ``gold.ba_daily_energy`` resolves as-is). Lake Formation applies to the caller's role."""

    strict = False

    def __init__(
        self,
        workgroup: str,
        region: str,
        client: Any = None,
        timeout_s: float = 60.0,
        poll_s: float = 0.5,
    ):
        if client is None:
            import boto3  # optional dependency, only needed on AWS

            client = boto3.client("athena", region_name=region)
        self.client, self.workgroup = client, workgroup
        self.timeout_s, self.poll_s = timeout_s, poll_s

    def run(self, sql: str, tables: set[str], limit: int) -> tuple[list[str], list[list]]:
        qid = self.client.start_query_execution(
            QueryString=sql,
            WorkGroup=self.workgroup,
            QueryExecutionContext={"Catalog": "AwsDataCatalog"},
        )["QueryExecutionId"]
        deadline = time.monotonic() + self.timeout_s
        while True:
            status = self.client.get_query_execution(QueryExecutionId=qid)["QueryExecution"][
                "Status"
            ]
            if status["State"] == "SUCCEEDED":
                break
            if status["State"] in ("FAILED", "CANCELLED"):
                raise RuntimeError(status.get("StateChangeReason", status["State"]))
            if time.monotonic() > deadline:
                self.client.stop_query_execution(QueryExecutionId=qid)
                raise TimeoutError(f"query {qid} exceeded {self.timeout_s:.0f}s")
            time.sleep(self.poll_s)
        columns: list[str] = []
        rows: list[list] = []
        pages = self.client.get_paginator("get_query_results").paginate(QueryExecutionId=qid)
        for page in pages:
            if not columns:
                columns = [c["Name"] for c in page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
            for r in page["ResultSet"]["Rows"]:
                values = [d.get("VarCharValue") for d in r["Data"]]
                if values == columns and not rows:  # Athena repeats the header as row one
                    continue
                rows.append(values)
                if len(rows) > limit:
                    return columns, rows
        return columns, rows


# -- document retrieval -------------------------------------------------------


class Retriever(Protocol):
    def search(
        self, query: str, clearance: str, filters: dict[str, str], k: int
    ) -> list[dict[str, Any]]: ...


class LocalRetriever:
    def __init__(self, load: Callable[[], VectorIndex]):
        self._load, self._index = load, None

    def search(
        self, query: str, clearance: str, filters: dict[str, str], k: int
    ) -> list[dict[str, Any]]:
        self._index = self._index or self._load()
        return [
            {
                "doc_id": h.document.doc_id,
                "source_table": h.document.metadata.get("source_table"),
                "classification": h.document.classification,
                "score": round(h.score, 4),
                "text": h.document.text,
            }
            for h in self._index.search(query, k=k, clearance=clearance, filters=filters)
        ]


def bedrock_filter(clearance: str, filters: dict[str, str]) -> dict[str, Any]:
    """Bedrock Knowledge Base metadata filter: classification within clearance, plus equals."""
    clauses: list[dict[str, Any]] = [
        {"in": {"key": "classification", "value": sorted(allowed_classifications(clearance))}}
    ]
    clauses += [{"equals": {"key": k, "value": v}} for k, v in sorted(filters.items())]
    return clauses[0] if len(clauses) == 1 else {"andAll": clauses}


class BedrockKbRetriever:
    """``bedrock-agent-runtime:Retrieve`` with the clearance filter applied server-side. Results
    are checked again on return, so a mis-tagged document still never reaches the model."""

    def __init__(self, knowledge_base_id: str, region: str, client: Any = None):
        if client is None:
            import boto3  # optional dependency, only needed on AWS

            client = boto3.client("bedrock-agent-runtime", region_name=region)
        self.client, self.kb_id = client, knowledge_base_id

    def search(
        self, query: str, clearance: str, filters: dict[str, str], k: int
    ) -> list[dict[str, Any]]:
        resp = self.client.retrieve(
            knowledgeBaseId=self.kb_id,
            retrievalQuery={"text": query},
            retrievalConfiguration={
                "vectorSearchConfiguration": {
                    "numberOfResults": k,
                    "filter": bedrock_filter(clearance, filters),
                }
            },
        )
        allowed = allowed_classifications(clearance)
        out = []
        for r in resp.get("retrievalResults", []):
            md = r.get("metadata") or {}
            if md.get("classification") not in allowed:
                continue
            out.append(
                {
                    "doc_id": md.get("doc_id") or r.get("location", {}),
                    "source_table": md.get("source_table"),
                    "classification": md["classification"],
                    "score": r.get("score"),
                    "text": r["content"]["text"],
                }
            )
        return out


# -- the toolbox --------------------------------------------------------------

SPECS: list[dict[str, Any]] = [
    {
        "name": "list_tables",
        "description": (
            "List the governed tables you may query, with layer, grain, description and "
            "columns. Call this before writing SQL if you are unsure of a column name. Gold "
            "tables are the data products to prefer for answers."
        ),
        "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "run_sql",
        "description": (
            "Run one read-only SELECT over the governed tables and return up to 50 rows. Name "
            "tables as namespace.table, e.g. gold.ba_daily_energy. Use portable SQL that works "
            "on both DuckDB and Athena: DATE 'YYYY-MM-DD' literals, round(), avg(), GROUP BY, "
            "ORDER BY, LIMIT. Tables above the caller's clearance are refused."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"sql": {"type": "string", "description": "A single SELECT statement"}},
            "required": ["sql"],
            "additionalProperties": False,
        },
    },
    {
        "name": "describe_table",
        "description": (
            "Catalog entry for one table: owner, classification, primary key, quality rules, "
            "upstream lineage and consumers. Use it to say which governed table backs a "
            "number and who owns it."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"table": {"type": "string", "description": "e.g. gold.fuel_mix_daily"}},
            "required": ["table"],
            "additionalProperties": False,
        },
    },
    {
        "name": "lookup_entity",
        "description": (
            "Resolve names in free text (balancing authorities and their aliases such as "
            "'California ISO' or 'ERCOT', regions, interconnections, energy sources, business "
            "terms) to knowledge-graph entities, with codes and facts such as generation mix."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"text": {"type": "string", "description": "Text naming entities"}},
            "required": ["text"],
            "additionalProperties": False,
        },
    },
    {
        "name": "search_documents",
        "description": (
            "Semantic search over knowledge-base passages: daily facts per balancing "
            "authority, cross-BA rankings, reliability summaries and table catalog entries. "
            "Optional exact-match filters narrow the search."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "ba_code": {"type": "string", "description": "e.g. CISO"},
                "date": {"type": "string", "description": "YYYY-MM-DD"},
                "doc_type": {"type": "string", "enum": ["fact", "catalog"]},
                "k": {"type": "integer", "description": "number of passages, 1-8"},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
    },
]


class Toolbox:
    def __init__(
        self,
        contracts: dict[str, Contract],
        clearance: str,
        sql: SqlEngine,
        retriever: Retriever | None = None,
        graph: Callable[[], KnowledgeGraph] | None = None,
        max_rows: int = MAX_ROWS,
    ):
        if clearance not in ("public", "internal", "confidential"):
            raise ValueError(f"unknown clearance {clearance!r}")
        self.contracts, self.clearance = contracts, clearance
        self.sql, self.retriever, self.max_rows = sql, retriever, max_rows
        self._graph_factory, self._graph = graph, None

    @property
    def graph(self) -> KnowledgeGraph:
        if self._graph is None:
            if self._graph_factory is None:
                raise RuntimeError("the knowledge graph is not configured")
            self._graph = self._graph_factory()
        return self._graph

    def specs(self) -> list[dict[str, Any]]:
        return SPECS

    def call(self, name: str, args: dict[str, Any]) -> ToolResult:
        handler = getattr(self, f"_tool_{name}", None)
        if handler is None or name not in {s["name"] for s in self.specs()}:
            return ToolResult({"error": f"unknown tool {name!r}"}, is_error=True)
        try:
            return handler(**(args or {}))
        except AccessDenied as e:
            return ToolResult({"error": f"access denied: {e}"}, is_error=True)
        except (ValueError, TypeError, KeyError) as e:
            return ToolResult({"error": f"invalid request: {e}"}, is_error=True)
        except Exception as e:  # surface engine failures to the model instead of crashing
            return ToolResult({"error": f"{type(e).__name__}: {e}"}, is_error=True)

    # -- access ---------------------------------------------------------------
    def _contract(self, table: str) -> Contract:
        c = self.contracts.get(table.strip().lower())
        if c is None:
            raise ValueError(f"unknown table {table!r}; call list_tables")
        return c

    def _check_rows(self, c: Contract) -> None:
        if not cleared(c.classification, self.clearance):
            raise AccessDenied(
                f"{c.name} is classified {c.classification}; caller clearance is {self.clearance}"
            )

    def _check_metadata(self, c: Contract) -> None:
        needed = metadata_classification(c.classification)
        if not cleared(needed, self.clearance):
            raise AccessDenied(
                f"catalog metadata for {c.name} needs {needed} clearance; caller has {self.clearance}"
            )

    # -- tools ----------------------------------------------------------------
    def _tool_list_tables(self) -> ToolResult:
        order = {"gold": 0, "silver": 1, "bronze": 2, "ops": 3}
        tables = [
            {
                "table": c.name,
                "layer": c.layer,
                "grain": list(c.primary_key),
                "description": " ".join(c.description.split()),
                "columns": [
                    {
                        "name": col.name,
                        "type": col.type,
                        **({"about": col.description} if col.description else {}),
                    }
                    for col in c.columns
                    if not col.name.startswith("_")
                ],
            }
            for c in sorted(self.contracts.values(), key=lambda c: (order.get(c.layer, 9), c.name))
            if cleared(c.classification, self.clearance)
        ]
        return ToolResult({"clearance": self.clearance, "tables": tables})

    def _tool_run_sql(self, sql: str) -> ToolResult:
        tables = referenced_tables(sql, strict=self.sql.strict)
        contracts = [self._contract(t) for t in sorted(tables)]
        for c in contracts:
            self._check_rows(c)
        columns, rows = self.sql.run(sql, tables, self.max_rows)
        truncated = len(rows) > self.max_rows
        return ToolResult(
            {"columns": columns, "rows": rows[: self.max_rows], "truncated": truncated},
            tables=[c.name for c in contracts],
        )

    def _tool_describe_table(self, table: str) -> ToolResult:
        c = self._contract(table)
        self._check_metadata(c)
        upstream: list[str] = []
        if self._graph_factory is not None:
            kind = "DataProduct" if c.layer == "gold" else "Dataset"
            upstream = [
                self.graph.label(i)
                for i in self.graph.upstream(self.graph.entity(kind, c.name))
                if self.graph.visible(i, self.clearance)
            ]
        return ToolResult(
            {
                "table": c.name,
                "layer": c.layer,
                "classification": c.classification,
                "owner": c.owner,
                "contract_version": c.version,
                "description": " ".join(c.description.split()),
                "primary_key": list(c.primary_key),
                "quality_rules": [
                    r["rule"] + (f"({r['column']})" if r.get("column") else "") for r in c.quality
                ],
                "derived_from": upstream,
                "consumers": list(c.consumers),
            },
            tables=[c.name],
        )

    def _tool_lookup_entity(self, text: str) -> ToolResult:
        g = self.graph
        entities = []
        for iri in link_entities(g, text):
            if not g.visible(iri, self.clearance):
                continue
            codes = [o.value for o in g.objects(iri, "code") if isinstance(o, Literal)]
            entities.append(
                {
                    "label": g.label(iri),
                    "type": sorted(g._types.get(iri, {"Thing"}))[0],
                    **({"code": codes[0]} if codes else {}),
                    "facts": [f for f in g.describe(iri, self.clearance) if "has column" not in f][
                        :25
                    ],
                }
            )
        return ToolResult({"entities": entities}, documents=["knowledge-graph"] if entities else [])

    def _tool_search_documents(
        self,
        query: str,
        ba_code: str | None = None,
        date: str | None = None,
        doc_type: str | None = None,
        k: int = 4,
    ) -> ToolResult:
        if self.retriever is None:
            raise RuntimeError("document search is not configured")
        filters = {
            key: v for key, v in (("ba_code", ba_code), ("date", date), ("doc_type", doc_type)) if v
        }
        hits = self.retriever.search(query, self.clearance, filters, max(1, min(int(k), 8)))
        return ToolResult(
            {"passages": hits},
            # Fact passages carry rows of their source table; catalog passages only describe one.
            tables=sorted(
                {
                    h["source_table"]
                    for h in hits
                    if h.get("source_table") and not h["doc_id"].startswith("catalog/")
                }
            ),
            documents=[str(h["doc_id"]) for h in hits],
        )
