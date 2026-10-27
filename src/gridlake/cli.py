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
    settings = Settings()
    source = None
    if args.inject_fault or args.grid_events:
        from gridlake.sources.eia import SyntheticEIASource, load_balancing_authorities

        source = SyntheticEIASource(load_balancing_authorities(settings.seeds_dir))
    if args.grid_events:
        from gridlake.watcher.scenarios import GridEventSource

        source = GridEventSource(source)
    if args.inject_fault:
        from gridlake.reliability.faults import FaultySource

        source = FaultySource(source, args.inject_fault)
    pipeline = Pipeline(settings, source=source)
    try:
        summary = pipeline.run(
            start,
            end,
            telemetry_hours=args.telemetry_hours,
            produce_telemetry=not args.no_telemetry,
            reconcile_late=args.reconcile,
        )
    except Exception as exc:  # the manifest is written; report the failure like a scheduler would
        from gridlake.pipeline import RunSummary

        summary = RunSummary(pipeline.run_id, "", pipeline.steps)
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
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
    settings = Settings()
    if args.context_only:
        from gridlake.knowledge.context import build_context
        from gridlake.knowledge.retrieval import VectorIndex
        from gridlake.pipeline import Pipeline

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

    from gridlake.agent import build_analyst

    answer = build_analyst(settings, args.clearance, llm=args.llm).ask(args.question)
    if args.json:
        print(json.dumps(answer.to_dict(), indent=2, default=str))
        return 0
    if args.trace:
        for step in answer.steps:
            status = "ok" if step.ok else f"error: {step.error}"
            print(f"> {step.tool}({json.dumps(step.input)}) -> {status}")
        print()
    print(answer.text)
    print(f"\nsources: tables={', '.join(answer.tables) or 'none'}", end="")
    print(
        f"; documents={', '.join(answer.documents) or 'none'}  [{answer.model}, clearance={answer.clearance}]"
    )
    return 0


def cmd_replay(args: argparse.Namespace) -> int:
    from gridlake.pipeline import Pipeline

    pipeline = Pipeline(Settings())
    started = datetime.now(UTC).isoformat()
    try:
        pipeline.replay_quarantine(args.table, args.rule)
    finally:
        summary = pipeline._write_manifest(started)
    for s in summary.steps:
        print(f"{s.status:<9} {s.name:<30} {json.dumps(s.metrics, default=str)}")
    return 0 if summary.ok else 1


def cmd_triage(args: argparse.Namespace) -> int:
    from gridlake.reliability import triage_run

    run, triages = triage_run(
        Settings(),
        run_id=args.run,
        clearance=args.clearance,
        llm=args.llm,
        min_rate=args.min_rate,
        min_rows=args.min_rows,
    )
    if args.json:
        print(json.dumps([t.to_dict() for t in triages], indent=2, default=str))
        return 0
    print(f"run {run.run_id} ({run.status}, started {run.started}): {len(triages)} incident(s)")
    for t in triages:
        print(f"\n== {t.incident.id}: {t.incident.headline()}")
        if args.trace:
            for step in t.steps:
                status = "ok" if step.ok else f"error: {step.error}"
                print(f"> {step.tool}({json.dumps(step.input)[:160]}) -> {status}")
        print(t.summary)
        for d in t.drafts:
            print(f"  draft {d.id} [{d.kind}] {d.title}")
    if triages:
        print("\nreview: gridlake incidents show <draft>; then approve or reject it")
    return 0


def cmd_incidents(args: argparse.Namespace) -> int:
    import getpass
    import os
    from pathlib import Path

    from gridlake.reliability import draft_store
    from gridlake.reliability.drafts import GitHubIssues, approve, reject

    settings = Settings()
    store = draft_store(settings)
    if args.action == "list":
        for d in store.list(None if args.all else "pending"):
            print(f"{d.id:<28} {d.status:<9} {d.kind:<6} {d.title}")
        return 0
    if not args.draft:
        print("name a draft id", file=sys.stderr)
        return 2
    if args.action == "show":
        print(store.get(args.draft).render())
        return 0
    who = args.by or os.getenv("GRIDLAKE_APPROVER") or getpass.getuser()
    if args.action == "reject":
        print(reject(store, args.draft, who, args.reason).render())
        return 0
    repo, token = settings.github_repo, os.getenv("GITHUB_TOKEN")
    github = GitHubIssues(repo, token) if repo and token else None
    root = Path(args.repo_root).resolve()
    draft = approve(
        store, args.draft, who, settings.incidents_dir / "approved", root, github, args.apply
    )
    print(f"{draft.id} approved by {draft.decided_by}: {draft.outcome}")
    return 0


def cmd_watch(args: argparse.Namespace) -> int:
    from gridlake.watcher import BriefStore, announce, watch

    settings = Settings()
    brief = watch(
        settings,
        day=_day(args.date).date() if args.date else None,
        llm=args.llm,
        clearance=args.clearance,
        lookback=args.lookback,
        z_min=args.z,
    )
    location = (
        None
        if args.no_save
        else BriefStore(settings.briefs_dir, settings.briefs_s3_uri).save(brief)
    )
    if args.announce and location:
        announce(settings, brief, location)
    if args.json:
        print(json.dumps(brief.to_dict(), indent=2, default=str))
        return 0
    if args.trace:
        for step in brief.steps:
            status = "ok" if step.ok else f"error: {step.error}"
            print(f"> {step.tool}({json.dumps(step.input)[:160]}) -> {status}")
        print()
    print(brief.markdown())
    if location:
        print(f"saved: {location}")
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
    run.add_argument(
        "--inject-fault",
        choices=["new_fuel_type", "unit_change", "feed_outage"],
        help="demo: inject a realistic source fault for `gridlake triage` to diagnose",
    )
    run.add_argument(
        "--grid-events",
        action="store_true",
        help="demo: plant a heatwave, a wind lull and a hydro shortfall on the last day",
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

    ask = sub.add_parser("ask", help="ask the grid analyst agent a question (cites its tables)")
    ask.add_argument("question")
    ask.add_argument(
        "--clearance", choices=["public", "internal", "confidential"], default="public"
    )
    ask.add_argument(
        "--llm",
        choices=["offline", "bedrock"],
        help="model driving the agent (default: GRIDLAKE_AGENT_LLM, else offline)",
    )
    ask.add_argument("--trace", action="store_true", help="print each tool call")
    ask.add_argument(
        "--context-only",
        action="store_true",
        help="print the GraphRAG context pack instead of running the agent",
    )
    ask.add_argument("-k", type=int, default=4, help="passages in the context pack")
    ask.add_argument("--json", action="store_true")
    ask.set_defaults(func=cmd_ask)

    replay = sub.add_parser(
        "replay", help="re-check quarantined silver rows after a fix and publish those that pass"
    )
    replay.add_argument(
        "table",
        choices=[
            "silver.demand_hourly",
            "silver.generation_hourly",
            "silver.grid_telemetry_minute",
        ],
    )
    replay.add_argument("--rule", help="only rows that failed this rule")
    replay.set_defaults(func=cmd_replay)

    triage = sub.add_parser(
        "triage", help="data reliability agent: diagnose a run's incidents and draft fixes"
    )
    triage.add_argument("--run", help="run id or prefix (default: the latest run)")
    triage.add_argument(
        "--clearance", choices=["public", "internal", "confidential"], default="internal"
    )
    triage.add_argument("--llm", choices=["offline", "bedrock"])
    triage.add_argument(
        "--min-rate", type=float, default=0.02, help="share of rows a rule must reject"
    )
    triage.add_argument("--min-rows", type=int, default=5)
    triage.add_argument("--trace", action="store_true", help="print each tool call")
    triage.add_argument("--json", action="store_true")
    triage.set_defaults(func=cmd_triage)

    incidents = sub.add_parser("incidents", help="review, approve or reject drafted remediations")
    incidents.add_argument("action", choices=["list", "show", "approve", "reject"])
    incidents.add_argument("draft", nargs="?")
    incidents.add_argument("--all", action="store_true", help="list decided drafts too")
    incidents.add_argument("--by", help="who is deciding (default: $GRIDLAKE_APPROVER or $USER)")
    incidents.add_argument("--reason", default="", help="why a draft is rejected")
    incidents.add_argument(
        "--apply", action="store_true", help="approve: git apply a fix to --repo-root"
    )
    incidents.add_argument("--repo-root", default=".")
    incidents.set_defaults(func=cmd_incidents)

    watch_ = sub.add_parser(
        "watch", help="grid anomaly watcher: flag and explain unusual days, write the daily brief"
    )
    watch_.add_argument("--date", help="UTC day YYYY-MM-DD (default: latest complete day)")
    watch_.add_argument("--lookback", type=int, default=28, help="days of history to compare")
    watch_.add_argument("--z", type=float, default=3.5, help="robust z-score to flag")
    watch_.add_argument(
        "--clearance", choices=["public", "internal", "confidential"], default="public"
    )
    watch_.add_argument("--llm", choices=["offline", "bedrock"])
    watch_.add_argument("--trace", action="store_true", help="print each tool call")
    watch_.add_argument("--json", action="store_true")
    watch_.add_argument("--no-save", action="store_true", help="do not write the brief files")
    watch_.add_argument(
        "--announce", action="store_true", help="also post it to GRIDLAKE_BRIEF_TOPIC_ARN"
    )
    watch_.set_defaults(func=cmd_watch)

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
