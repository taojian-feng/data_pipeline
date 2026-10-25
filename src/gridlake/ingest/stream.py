"""Streaming ingestion with at-least-once delivery and exactly-once results.

Locally the "topic" is a file-backed partitioned log with Kafka/Kinesis semantics
(ordered per partition, consumer-group offsets). On AWS the producer writes to a
Kinesis Data Stream and the same consumer logic runs in a Glue streaming job.

Guarantees:
  * Offsets are committed only *after* the batch is durably committed to Iceberg,
    so a crash replays the batch (at-least-once) rather than losing it.
  * Duplicates from replay or redelivery are removed in silver by ``event_id``,
    making the end result exactly-once.
  * An event-time watermark flags late events. As in Flink / Spark Structured
    Streaming, it is the *minimum* over partitions of each partition's max event time,
    minus the allowed lateness, so a fast partition cannot make a slow one look late. They are never dropped: they land in bronze with ``_is_late`` and are
    folded in by the reconciliation pass.
"""

from __future__ import annotations

import json
import zlib
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa


class FileStreamLog:
    """Append-only partitioned JSONL log (local stand-in for Kinesis / Kafka)."""

    def __init__(self, root: Path, topic: str, partitions: int = 4):
        self.dir = root / topic
        self.dir.mkdir(parents=True, exist_ok=True)
        self.partitions = partitions

    def _path(self, p: int) -> Path:
        return self.dir / f"partition-{p}.jsonl"

    def partition_for(self, key: str) -> int:
        return zlib.crc32(key.encode()) % self.partitions

    def produce(self, records: Iterable[dict], key_field: str) -> int:
        handles = {p: self._path(p).open("a") for p in range(self.partitions)}
        n = 0
        try:
            for r in records:
                handles[self.partition_for(r[key_field])].write(json.dumps(r) + "\n")
                n += 1
        finally:
            for h in handles.values():
                h.close()
        return n

    def read(self, partition: int, offset: int, max_records: int) -> list[tuple[int, dict]]:
        path = self._path(partition)
        if not path.exists():
            return []
        out = []
        with path.open() as f:
            for i, line in enumerate(f):
                if i < offset:
                    continue
                if len(out) >= max_records:
                    break
                out.append((i, json.loads(line)))
        return out


class OffsetStore:
    """Consumer-group checkpoint (DynamoDB / Kinesis checkpoint table on AWS)."""

    def __init__(self, path: Path):
        self.path = path

    def load(self) -> dict[int, int]:
        if not self.path.exists():
            return {}
        return {int(k): v for k, v in json.loads(self.path.read_text()).items()}

    def commit(self, offsets: dict[int, int]) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(offsets))
        tmp.replace(self.path)  # atomic


@dataclass
class MicroBatch:
    table: pa.Table
    next_offsets: dict[int, int]
    late_count: int
    watermark: datetime | None


class StreamConsumer:
    def __init__(
        self,
        log: FileStreamLog,
        offsets: OffsetStore,
        batch_size: int = 5000,
        allowed_lateness: timedelta = timedelta(minutes=10),
    ):
        self.log = log
        self.offsets = offsets
        self.batch_size = batch_size
        self.allowed_lateness = allowed_lateness
        self.partition_max: dict[int, datetime] = {}

    def batches(self) -> Iterator[MicroBatch]:
        committed = self.offsets.load()
        while True:
            rows, next_offsets = [], dict(committed)
            per_partition = max(1, self.batch_size // self.log.partitions)
            for p in range(self.log.partitions):
                for offset, rec in self.log.read(p, committed.get(p, 0), per_partition):
                    rows.append((p, offset, rec))
                    next_offsets[p] = offset + 1
            if not rows:
                return
            yield self._to_batch(rows, next_offsets)
            # In-memory progress only; the durable checkpoint moves when the caller
            # calls commit() after the Iceberg write succeeds.
            committed = next_offsets

    def _to_batch(
        self, rows: list[tuple[int, int, dict]], next_offsets: dict[int, int]
    ) -> MicroBatch:
        now = datetime.now(UTC)
        cols: dict[str, list] = {
            k: []
            for k in (
                "event_id",
                "substation_id",
                "ba_code",
                "event_ts",
                "frequency_hz",
                "load_mw",
                "voltage_kv",
                "_partition",
                "_offset",
                "_is_late",
                "_ingest_ts",
            )
        }
        late = 0
        watermark = self.watermark()  # fixed for the batch, advanced after it
        for p, offset, r in rows:
            ts = datetime.fromisoformat(r["event_ts"])
            is_late = watermark is not None and ts < watermark
            late += is_late
            if p not in self.partition_max or ts > self.partition_max[p]:
                self.partition_max[p] = ts
            for k in (
                "event_id",
                "substation_id",
                "ba_code",
                "frequency_hz",
                "load_mw",
                "voltage_kv",
            ):
                cols[k].append(r.get(k))
            cols["event_ts"].append(ts)
            cols["_partition"].append(str(p))
            cols["_offset"].append(f"{offset:020d}")  # zero-padded so it sorts like a number
            cols["_is_late"].append(is_late)
            cols["_ingest_ts"].append(now)
        return MicroBatch(pa.table(cols), next_offsets, late, self.watermark())

    def watermark(self) -> datetime | None:
        # Idle (never-seen) partitions are ignored, like an idleness timeout in Flink.
        if not self.partition_max:
            return None
        return min(self.partition_max.values()) - self.allowed_lateness

    def commit(self, batch: MicroBatch) -> None:
        self.offsets.commit(batch.next_offsets)
