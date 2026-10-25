"""Command-line entry point: ``gridlake --help``."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, datetime, timedelta

from gridlake.config import Settings


def _day(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=UTC)


def cmd_run(args: argparse.Namespace) -> int:
    from gridlake.pipeline import Pipeline

    end = (
        _day(args.end)
        if args.end
        else datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    )
    start = _day(args.start) if args.start else end - timedelta(days=args.days)
    summary = Pipeline(Settings()).run(
        start,
        end,
        telemetry_hours=args.telemetry_hours,
        produce_telemetry=not args.no_telemetry,
        reconcile_late=args.reconcile,
    )
    width = max(len(s.name) for s in summary.steps)
    for s in summary.steps:
        print(
            f"{s.status:<9} {s.name:<{width}}  {s.seconds:>6.2f}s  {json.dumps(s.metrics, default=str)}"
        )
    print(f"\nrun {summary.run_id}: {'OK' if summary.ok else 'FAILED'}")
    return 0 if summary.ok else 1


def cmd_step(args: argparse.Namespace) -> int:
    from gridlake.pipeline import Pipeline

    end = (
        _day(args.end) if args.end else datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    )
    start = _day(args.start) if args.start else end - timedelta(days=args.days)
    since = datetime.fromisoformat(args.since) if args.since else None
    summary = Pipeline(Settings()).run_step(
        args.step,
        start,
        end,
        since=since,
        telemetry_hours=args.telemetry_hours,
        produce_telemetry=not args.no_telemetry,
        reconcile_late=args.reconcile,
    )
    print(json.dumps(summary.to_dict(), default=str))
    return 0 if summary.ok else 1


def cmd_sql(args: argparse.Namespace) -> int:
    """Ad-hoc SQL over Iceberg tables: tables are exposed as <namespace>_<table>."""
    from gridlake.catalog import Lakehouse, get_catalog
    from gridlake.contracts import load_contracts
    from gridlake.transform.sql import connect

    settings = Settings()
    lake = Lakehouse(get_catalog(settings), load_contracts(settings.contracts_dir))
    names = [n for n in lake.contracts if n.replace(".", "_") in args.query]
    con = connect(**{n.replace(".", "_"): lake.read(n) for n in names})
    con.sql(args.query).show(max_width=200)
    return 0


def cmd_ask(args: argparse.Namespace) -> int:
    from gridlake.knowledge.context import build_context
    from gridlake.knowledge.retrieval import VectorIndex
    from gridlake.pipeline import Pipeline

    settings = Settings()
    index = VectorIndex.load(settings.knowledge_dir / "index")
    pack = build_context(
        Pipeline(settings).knowledge_graph(),
        index,
        args.question,
        clearance=args.clearance,
        k=args.k,
    )
    print(json.dumps(pack.__dict__, indent=2, default=str) if args.json else pack.to_prompt())
    return 0


def cmd_lineage(args: argparse.Namespace) -> int:
    from gridlake import lineage as ol

    for src, job, dst in ol.read_edges(Settings().lineage_dir):
        print(f"{src:<36} --[{job}]--> {dst}")
    return 0


def cmd_quality(args: argparse.Namespace) -> int:
    from gridlake.catalog import Lakehouse, get_catalog
    from gridlake.contracts import load_contracts
    from gridlake.transform.sql import connect, query

    settings = Settings()
    lake = Lakehouse(get_catalog(settings), load_contracts(settings.contracts_dir))
    results = lake.read("quality.results")
    out = query(
        """SELECT table_name, rule, severity, sum(failed_rows) AS failed_rows, sum(total_rows) AS rows_checked,
                  bool_and(passed) AS always_passed
           FROM r GROUP BY ALL ORDER BY always_passed, table_name, rule""",
        r=results,
    )
    connect(out=out).sql("SELECT * FROM out").show(max_rows=100, max_width=200)
    q = lake.read("quality.quarantine")
    print(f"\nquarantined rows: {q.num_rows}")
    for row in q.slice(0, args.samples).to_pylist():
        print(f"  {row['table_name']}: {row['failed_rules']} :: {row['record_json'][:140]}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="gridlake", description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run the end-to-end pipeline")
    run.add_argument("--start", help="UTC start date YYYY-MM-DD (default: end - days)")
    run.add_argument("--end", help="UTC end date YYYY-MM-DD, exclusive (default: today)")
    run.add_argument("--days", type=int, default=14)
    run.add_argument("--telemetry-hours", type=int, default=6)
    run.add_argument(
        "--no-telemetry", action="store_true", help="do not simulate telemetry producers"
    )
    run.add_argument(
        "--reconcile", action="store_true", help="fold late stream events into silver/gold"
    )
    run.set_defaults(func=cmd_run)

    step = sub.add_parser(
        "step", help="run a single stage (how Step Functions invokes the pipeline)"
    )
    step.add_argument(
        "step", choices=["seed", "ingest_eia", "ingest_stream", "silver", "gold", "knowledge"]
    )
    step.add_argument("--start")
    step.add_argument("--end")
    step.add_argument("--days", type=int, default=3)
    step.add_argument(
        "--since", help="ISO timestamp; gold rebuilds dates whose silver rows changed since then"
    )
    step.add_argument("--telemetry-hours", type=int, default=6)
    step.add_argument("--no-telemetry", action="store_true")
    step.add_argument("--reconcile", action="store_true")
    step.set_defaults(func=cmd_step)

    sql = sub.add_parser(
        "sql", help='query tables, e.g. "SELECT * FROM gold_ba_daily_energy LIMIT 5"'
    )
    sql.add_argument("query")
    sql.set_defaults(func=cmd_sql)

    ask = sub.add_parser("ask", help="build a grounded GraphRAG context pack for a question")
    ask.add_argument("question")
    ask.add_argument(
        "--clearance", choices=["public", "internal", "confidential"], default="public"
    )
    ask.add_argument("-k", type=int, default=4)
    ask.add_argument("--json", action="store_true")
    ask.set_defaults(func=cmd_ask)

    sub.add_parser("lineage", help="print dataset lineage edges").set_defaults(func=cmd_lineage)
    quality = sub.add_parser("quality", help="data-quality report")
    quality.add_argument("--samples", type=int, default=5)
    quality.set_defaults(func=cmd_quality)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING, format="%(message)s"
    )
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
