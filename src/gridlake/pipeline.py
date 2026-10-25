"""Pipeline orchestration.

Steps run in dependency order. Each step is a lineage-tracked job: it emits OpenLineage
START/COMPLETE/FAIL events, every write passes a contract-driven quality gate, and the
run's metrics are persisted as a manifest. On AWS, each step maps to a Glue job / Lambda
task in the Step Functions state machine defined in ``infra/terraform/orchestration.tf``.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.expressions import In

from gridlake import lineage as ol
from gridlake.catalog import Lakehouse, get_catalog
from gridlake.config import Settings
from gridlake.contracts import load_contracts
from gridlake.ingest import batch
from gridlake.ingest.stream import FileStreamLog, OffsetStore, StreamConsumer
from gridlake.knowledge import graph as kg
from gridlake.knowledge import retrieval as rag
from gridlake.quality.engine import (
    QualityGateError,
    QualityReport,
    evaluate,
    quarantine_table,
    results_table,
)
from gridlake.sources.eia import (
    EIAClient,
    EIASource,
    SyntheticEIASource,
    load_balancing_authorities,
)
from gridlake.sources.telemetry import generate_events
from gridlake.transform import gold, silver

log = logging.getLogger("gridlake")
HWM = "gridlake.hwm."
TELEMETRY_TOPIC = "grid-telemetry"


@dataclass
class StepResult:
    name: str
    status: str
    seconds: float
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class RunSummary:
    run_id: str
    started: str
    steps: list[StepResult]

    @property
    def ok(self) -> bool:
        return all(s.status == "COMPLETE" for s in self.steps)

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "started": self.started,
            "ok": self.ok,
            "steps": [s.__dict__ for s in self.steps],
        }


def _dates_of(table: pa.Table, column: str) -> list[date]:
    if not table.num_rows:
        return []
    return sorted(
        {v.date() if isinstance(v, datetime) else v for v in table.column(column).to_pylist()}
    )


class Pipeline:
    def __init__(self, settings: Settings | None = None, source: EIASource | None = None):
        self.settings = settings or Settings()
        self.settings.ensure_dirs()
        self.contracts = load_contracts(self.settings.contracts_dir)
        self.lake = Lakehouse(get_catalog(self.settings), self.contracts)
        self.bas = load_balancing_authorities(self.settings.seeds_dir)
        if source is not None:
            self.source = source
        elif api_key := self.settings.resolve_eia_api_key():
            self.source = EIAClient(api_key, [b.ba_code for b in self.bas])
        else:
            self.source = SyntheticEIASource(self.bas)
        self.run_id = str(uuid.uuid4())
        self.lineage = ol.LineageEmitter(
            self.settings.lineage_dir, self.settings.openlineage_url, self.run_id
        )
        self.steps: list[StepResult] = []
        self._quality: dict[str, QualityReport] = {}

    # -- framework ----------------------------------------------------------
    def _step(
        self, job: str, inputs: list[str], outputs: list[str], fn: Callable[[], dict]
    ) -> dict:
        step_run = str(uuid.uuid4())
        self._quality = {}
        self.lineage.emit(
            "START",
            job,
            step_run,
            [ol.dataset(self.contracts[i]) for i in inputs],
            [ol.dataset(self.contracts[o]) for o in outputs],
        )
        t0 = time.perf_counter()
        try:
            metrics = fn() or {}
        except Exception as exc:
            self.lineage.emit("FAIL", job, step_run, [], [], error=f"{type(exc).__name__}: {exc}")
            self.steps.append(
                StepResult(job, "FAIL", round(time.perf_counter() - t0, 2), {"error": str(exc)})
            )
            raise
        out_ds = [
            ol.dataset(
                self.contracts[o],
                self._quality[o].metrics() if o in self._quality else None,
                self.lake.snapshot_id(o),
            )
            for o in outputs
        ]
        self.lineage.emit(
            "COMPLETE", job, step_run, [ol.dataset(self.contracts[i]) for i in inputs], out_ds
        )
        self.steps.append(StepResult(job, "COMPLETE", round(time.perf_counter() - t0, 2), metrics))
        log.info("%-28s %s", job, json.dumps(metrics, default=str))
        return metrics

    def publish(
        self,
        table: str,
        data: pa.Table,
        write: str = "upsert",
        overwrite_filter=None,
        mode: str = "quarantine",
    ) -> dict:
        """Quality-gate ``data`` against its contract, quarantine failures, then write."""
        contract = self.contracts[table]
        if data.num_rows == 0 and write != "overwrite":
            return {"rows": 0, "note": "no new data"}
        try:
            report = evaluate(contract, data, reference_loader=self.lake.read, mode=mode)
        except QualityGateError as exc:
            self.lake.append("quality.results", results_table(exc.report, self.run_id))
            raise
        self.lake.append("quality.results", results_table(report, self.run_id))
        if report.quarantined.num_rows:
            self.lake.append("quality.quarantine", quarantine_table(report, self.run_id))
        self._quality[table] = report
        if write == "append":
            written = self.lake.append(table, report.valid)
        elif write == "overwrite":
            written = self.lake.overwrite(table, report.valid, overwrite_filter)
        else:
            updated, inserted = self.lake.upsert(table, report.valid)
            written = updated + inserted
        warnings = [r.rule for r in report.results if not r.passed and r.severity == "warn"]
        return {
            "rows": written,
            "quarantined": report.quarantined.num_rows,
            **({"warnings": warnings} if warnings else {}),
        }

    def _new_rows(
        self, source_table: str, target_table: str
    ) -> tuple[pa.Table, pa.Table, datetime | None]:
        """Full source table, rows ingested after the target's high-water mark, and the new mark."""
        full = self.lake.read(source_table)
        props = self.lake.table(target_table).properties
        hwm = props.get(HWM + source_table)
        new = (
            full
            if hwm is None
            else full.filter(
                pc.greater(
                    full.column("_ingest_ts"),
                    pa.scalar(datetime.fromisoformat(hwm), pa.timestamp("us", tz="UTC")),
                )
            )
        )
        mark = pc.max(new.column("_ingest_ts")).as_py() if new.num_rows else None
        return full, new, mark

    def _advance(
        self, target_table: str, source_table: str, mark: datetime | None, suffix: str = ""
    ) -> None:
        if mark is not None:
            with self.lake.table(target_table).transaction() as tx:
                tx.set_properties({HWM + source_table + suffix: mark.isoformat()})

    # -- steps --------------------------------------------------------------
    def seed_dimensions(self) -> dict:
        def run():
            return {
                "dim_balancing_authority": self.publish(
                    "silver.dim_balancing_authority",
                    silver.dim_balancing_authority(self.settings.seeds_dir),
                ),
                "dim_fuel_type": self.publish(
                    "silver.dim_fuel_type", silver.dim_fuel_type(self.settings.seeds_dir)
                ),
            }

        return self._step(
            "seed_dimensions", [], ["silver.dim_balancing_authority", "silver.dim_fuel_type"], run
        )

    def ingest_eia(self, start: datetime, end: datetime) -> dict:
        def run():
            out = {}
            for table, fetch, key in (
                ("bronze.eia_region_data", self.source.region_data, "type"),
                ("bronze.eia_fuel_type_data", self.source.fuel_type_data, "fueltype"),
            ):
                records = fetch(start, end)
                bid = batch.batch_id(records, self.source.name)
                landed = batch.land(self.lake, table, records, key, self.source.name)
                if not landed["skipped"]:
                    evaluate(
                        self.contracts[table],
                        self.lake.read(table, row_filter=f"_batch_id == '{bid}'"),
                        mode="strict",
                    )
                out[table] = landed
            return out

        return self._step(
            "ingest_eia", [], ["bronze.eia_region_data", "bronze.eia_fuel_type_data"], run
        )

    def ingest_stream(self, start: datetime, end: datetime, produce: bool = True) -> dict:
        log_ = FileStreamLog(self.settings.stream_dir, TELEMETRY_TOPIC)
        consumer = StreamConsumer(
            log_, OffsetStore(self.settings.stream_dir / f"{TELEMETRY_TOPIC}.offsets.json")
        )

        def run():
            produced = (
                log_.produce(generate_events(self.bas, start, end), key_field="substation_id")
                if produce
                else 0
            )
            consumed = late = batches = 0
            for mb in consumer.batches():
                report = evaluate(self.contracts["bronze.grid_telemetry"], mb.table)
                self.lake.append("bronze.grid_telemetry", report.valid)  # durable write ...
                consumer.commit(mb)  # ... then checkpoint (at-least-once)
                consumed, late, batches = (
                    consumed + mb.table.num_rows,
                    late + mb.late_count,
                    batches + 1,
                )
            return {
                "produced": produced,
                "consumed": consumed,
                "micro_batches": batches,
                "late_events": late,
            }

        return self._step("ingest_stream", [], ["bronze.grid_telemetry"], run)

    def build_silver(self, reconcile_late: bool = False) -> dict[str, list[date]]:
        affected: dict[str, list[date]] = {}

        def demand():
            full, new, mark = self._new_rows("bronze.eia_region_data", "silver.demand_hourly")
            data = silver.demand_hourly(full, new)
            m = self.publish("silver.demand_hourly", data)
            self._advance("silver.demand_hourly", "bronze.eia_region_data", mark)
            affected["demand"] = _dates_of(data, "period")
            return m

        def generation():
            full, new, mark = self._new_rows(
                "bronze.eia_fuel_type_data", "silver.generation_hourly"
            )
            data = silver.generation_hourly(full, new)
            m = self.publish("silver.generation_hourly", data)
            self._advance("silver.generation_hourly", "bronze.eia_fuel_type_data", mark)
            affected["generation"] = _dates_of(data, "period")
            return m

        def telemetry():
            full, new, mark = self._new_rows(
                "bronze.grid_telemetry", "silver.grid_telemetry_minute"
            )
            rows = new
            if reconcile_late:
                # Also re-aggregate minutes touched by late events since the last reconciliation.
                since = self.lake.table("silver.grid_telemetry_minute").properties.get(
                    HWM + "bronze.grid_telemetry.reconcile"
                )
                pending = (
                    full
                    if since is None
                    else full.filter(
                        pc.greater(
                            full.column("_ingest_ts"),
                            pa.scalar(datetime.fromisoformat(since), pa.timestamp("us", tz="UTC")),
                        )
                    )
                )
                rows = pa.concat_tables([new, pending.filter(pending.column("_is_late"))])
            data = silver.grid_telemetry_minute(full, rows, reconcile=reconcile_late)
            m = self.publish("silver.grid_telemetry_minute", data)
            self._advance("silver.grid_telemetry_minute", "bronze.grid_telemetry", mark)
            if reconcile_late:
                self._advance(
                    "silver.grid_telemetry_minute",
                    "bronze.grid_telemetry",
                    pc.max(full.column("_ingest_ts")).as_py() if full.num_rows else None,
                    ".reconcile",
                )
            affected["telemetry"] = _dates_of(data, "minute_ts")
            return m

        self._step(
            "silver_demand_hourly",
            ["bronze.eia_region_data", "silver.dim_balancing_authority"],
            ["silver.demand_hourly"],
            demand,
        )
        self._step(
            "silver_generation_hourly",
            ["bronze.eia_fuel_type_data", "silver.dim_fuel_type", "silver.dim_balancing_authority"],
            ["silver.generation_hourly"],
            generation,
        )
        self._step(
            "silver_grid_telemetry_minute",
            ["bronze.grid_telemetry"],
            ["silver.grid_telemetry_minute"],
            telemetry,
        )
        return affected

    def build_gold(self, affected: dict[str, list[date]]) -> None:
        energy_dates = sorted(set(affected.get("demand", [])) | set(affected.get("generation", [])))
        demand = self.lake.read("silver.demand_hourly")
        generation = self.lake.read("silver.generation_hourly")
        dim_ba = self.lake.read("silver.dim_balancing_authority")
        dim_fuel = self.lake.read("silver.dim_fuel_type")

        def overwrite(table: str, data: pa.Table, dates: list[date]) -> dict:
            # dynamic partition overwrite of the affected `date` partitions only
            return self.publish(
                table,
                data,
                write="overwrite",
                mode="strict",
                overwrite_filter=In("date", [d.isoformat() for d in dates]),
            )

        if energy_dates:
            self._step(
                "gold_ba_daily_energy",
                [
                    "silver.demand_hourly",
                    "silver.generation_hourly",
                    "silver.dim_balancing_authority",
                    "silver.dim_fuel_type",
                ],
                ["gold.ba_daily_energy"],
                lambda: overwrite(
                    "gold.ba_daily_energy",
                    gold.ba_daily_energy(demand, generation, dim_ba, dim_fuel, energy_dates),
                    energy_dates,
                ),
            )
            self._step(
                "gold_fuel_mix_daily",
                ["silver.generation_hourly", "silver.dim_fuel_type"],
                ["gold.fuel_mix_daily"],
                lambda: overwrite(
                    "gold.fuel_mix_daily",
                    gold.fuel_mix_daily(generation, dim_fuel, energy_dates),
                    energy_dates,
                ),
            )
        if affected.get("demand"):
            # lags reach 7 days forward, so a changed day affects the following week's features
            horizon = sorted({d + timedelta(days=i) for d in affected["demand"] for i in range(8)})
            self._step(
                "gold_demand_forecast_features",
                ["silver.demand_hourly"],
                ["gold.demand_forecast_features"],
                lambda: self._features(demand, horizon),
            )
        if affected.get("telemetry"):
            tel_dates = affected["telemetry"]
            self._step(
                "gold_grid_reliability_daily",
                ["silver.grid_telemetry_minute"],
                ["gold.grid_reliability_daily"],
                lambda: overwrite(
                    "gold.grid_reliability_daily",
                    gold.grid_reliability_daily(
                        self.lake.read("silver.grid_telemetry_minute"), tel_dates
                    ),
                    tel_dates,
                ),
            )

    def _features(self, demand: pa.Table, dates: list[date]) -> dict:
        data = gold.demand_forecast_features(demand, dates)
        present = _dates_of(data, "period")
        if not present:
            return {"rows": 0}
        # Delete-then-insert the affected day partitions (period is day-partitioned).
        lo = datetime.combine(present[0], datetime.min.time(), UTC)
        hi = datetime.combine(present[-1] + timedelta(days=1), datetime.min.time(), UTC)
        row_filter = f"period >= '{lo.isoformat()}' and period < '{hi.isoformat()}'"
        return self.publish(
            "gold.demand_forecast_features",
            data,
            write="overwrite",
            mode="strict",
            overwrite_filter=row_filter,
        )

    def knowledge_graph(self) -> kg.KnowledgeGraph:
        telemetry = self.lake.read("silver.grid_telemetry_minute").select(
            ["substation_id", "ba_code"]
        )
        return kg.build_graph(
            kg.Ontology.load(),
            self.lake.read("silver.dim_balancing_authority").to_pylist(),
            self.lake.read("silver.dim_fuel_type").to_pylist(),
            self.contracts.values(),
            ol.read_edges(self.settings.lineage_dir),
            fuel_mix=self.lake.read("gold.fuel_mix_daily").to_pylist(),
            daily_energy=self.lake.read("gold.ba_daily_energy").to_pylist(),
            substations=sorted({(r["substation_id"], r["ba_code"]) for r in telemetry.to_pylist()}),
        )

    def build_knowledge(self) -> dict:
        def run():
            out_dir = self.settings.knowledge_dir
            graph = self.knowledge_graph()
            daily = self.lake.read("gold.ba_daily_energy").to_pylist()
            (out_dir / "ontology.ttl").write_text(graph.ont.to_turtle())
            (out_dir / "graph.ttl").write_text(graph.to_turtle())
            (out_dir / "graph.nt").write_text(graph.to_ntriples())

            upstream = {
                c: [
                    kg_iri.rsplit("/", 1)[-1]
                    for kg_iri in graph.upstream(
                        graph.entity(
                            "DataProduct" if self.contracts[c].layer == "gold" else "Dataset", c
                        )
                    )
                ]
                for c in self.contracts
            }
            docs = (
                rag.daily_energy_documents(
                    daily, self.contracts["gold.ba_daily_energy"].classification
                )
                + rag.summary_documents(
                    daily, self.contracts["gold.ba_daily_energy"].classification
                )
                + rag.reliability_documents(
                    self.lake.read("gold.grid_reliability_daily").to_pylist(),
                    self.contracts["gold.grid_reliability_daily"].classification,
                )
                + rag.catalog_documents(self.contracts.values(), upstream)
            )
            rag.VectorIndex().build(docs).save(out_dir / "index")
            kb_docs = rag.export_bedrock_kb(docs, out_dir / "bedrock_kb")
            if self.settings.kb_s3_uri:
                rag.upload_to_s3(out_dir / "bedrock_kb", self.settings.kb_s3_uri)
            return {"graph": graph.stats(), "documents": len(docs), "bedrock_kb_files": kb_docs * 2}

        inputs = [
            "gold.ba_daily_energy",
            "gold.fuel_mix_daily",
            "gold.grid_reliability_daily",
            "silver.dim_balancing_authority",
            "silver.dim_fuel_type",
            "silver.grid_telemetry_minute",
        ]
        return self._step("knowledge_layer", inputs, [], run)

    def affected_since(self, since: datetime) -> dict[str, list[date]]:
        """Dates whose silver rows changed at or after ``since`` (used when steps run separately)."""
        out = {}
        for key, table, column in (
            ("demand", "silver.demand_hourly", "period"),
            ("generation", "silver.generation_hourly", "period"),
            ("telemetry", "silver.grid_telemetry_minute", "minute_ts"),
        ):
            t = self.lake.read(table)
            changed = t.filter(
                pc.greater_equal(
                    t.column("_updated_ts"), pa.scalar(since, pa.timestamp("us", tz="UTC"))
                )
            )
            out[key] = _dates_of(changed, column)
        return out

    # -- entry points -------------------------------------------------------
    STEPS = ("seed", "ingest_eia", "ingest_stream", "silver", "gold", "knowledge")

    def run_step(
        self,
        step: str,
        start: datetime,
        end: datetime,
        since: datetime | None = None,
        telemetry_hours: int = 6,
        produce_telemetry: bool = True,
        reconcile_late: bool = False,
    ) -> RunSummary:
        """Run one stage, as a Step Functions task does on AWS. ``since`` scopes the gold rebuild."""
        started = datetime.now(UTC).isoformat()
        try:
            if step == "seed":
                self.seed_dimensions()
            elif step == "ingest_eia":
                self.ingest_eia(start, end)
            elif step == "ingest_stream":
                self.ingest_stream(
                    end - timedelta(hours=telemetry_hours), end, produce=produce_telemetry
                )
            elif step == "silver":
                self.build_silver(reconcile_late=reconcile_late)
            elif step == "gold":
                self.build_gold(self.affected_since(since or start))
            elif step == "knowledge":
                self.build_knowledge()
            else:
                raise ValueError(f"unknown step {step!r}; choose from {self.STEPS}")
        finally:
            summary = self._write_manifest(started)
        return summary

    def run(
        self,
        start: datetime,
        end: datetime,
        telemetry_hours: int = 6,
        produce_telemetry: bool = True,
        reconcile_late: bool = False,
    ) -> RunSummary:
        started = datetime.now(UTC).isoformat()
        try:
            self.seed_dimensions()
            self.ingest_eia(start, end)
            self.ingest_stream(
                end - timedelta(hours=telemetry_hours), end, produce=produce_telemetry
            )
            affected = self.build_silver(reconcile_late=reconcile_late)
            self.build_gold(affected)
            self.build_knowledge()
        finally:
            summary = self._write_manifest(started)
        return summary

    def _write_manifest(self, started: str) -> RunSummary:
        summary = RunSummary(self.run_id, started, self.steps)
        (self.settings.runs_dir / f"{self.run_id}.json").write_text(
            json.dumps(summary.to_dict(), indent=2, default=str)
        )
        return summary
