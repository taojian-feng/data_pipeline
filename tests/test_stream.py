from datetime import timedelta

from conftest import START
from gridlake.ingest.stream import FileStreamLog, OffsetStore, StreamConsumer


def make(tmp_path, n=20, partitions=2, late=None):
    log = FileStreamLog(tmp_path, "t", partitions=partitions)
    events = [
        {
            "event_id": f"e{i}",
            "substation_id": f"S{i % 4}",
            "ba_code": "CISO",
            "event_ts": (START + timedelta(minutes=i)).isoformat(),
            "frequency_hz": 60.0,
            "load_mw": 1.0,
            "voltage_kv": 230.0,
        }
        for i in range(n)
    ]
    if late:
        events.append({**events[0], "event_id": "late", "event_ts": (START - late).isoformat()})
    log.produce(events, key_field="substation_id")
    return log, OffsetStore(tmp_path / "offsets.json")


def test_consumes_everything_once_with_commits(tmp_path):
    log, offsets = make(tmp_path)
    c = StreamConsumer(log, offsets, batch_size=6)
    seen = []
    for mb in c.batches():
        seen += mb.table.column("event_id").to_pylist()
        c.commit(mb)
    assert sorted(seen) == sorted(f"e{i}" for i in range(20))
    assert list(StreamConsumer(log, offsets).batches()) == []  # nothing left after restart


def test_uncommitted_batch_is_replayed_after_crash(tmp_path):
    log, offsets = make(tmp_path)
    first = next(StreamConsumer(log, offsets, batch_size=4).batches())  # "crash" before commit
    replay = next(StreamConsumer(log, offsets, batch_size=4).batches())
    assert first.table.column("event_id").to_pylist() == replay.table.column("event_id").to_pylist()


def test_watermark_flags_late_events(tmp_path):
    log, offsets = make(tmp_path, late=timedelta(hours=2))
    c = StreamConsumer(log, offsets, batch_size=10, allowed_lateness=timedelta(minutes=5))
    late = {}
    for mb in c.batches():
        late.update(
            zip(
                mb.table.column("event_id").to_pylist(),
                mb.table.column("_is_late").to_pylist(),
                strict=True,
            )
        )
    assert late["late"] is True
    assert sum(late.values()) == 1
