"""Substation telemetry event generator.

Emulates what arrives on a real at-least-once stream: redelivered duplicates,
out-of-order events, late arrivals and occasional frequency excursions.
"""

from __future__ import annotations

import random
import uuid
from collections.abc import Iterable
from datetime import datetime, timedelta

from gridlake.sources.eia import BalancingAuthority

NOMINAL_HZ = 60.0


def generate_events(
    bas: Iterable[BalancingAuthority],
    start: datetime,
    end: datetime,
    interval_s: int = 15,
    substations_per_ba: int = 3,
    seed: int = 7,
    duplicate_rate: float = 0.02,
    late_rate: float = 0.005,
) -> list[dict]:
    rng = random.Random(seed)
    events: list[dict] = []
    for ba in bas:
        for n in range(1, substations_per_ba + 1):
            sub = f"{ba.ba_code}-SS{n:02d}"
            base_load = ba.peak_scale_mw / (substations_per_ba * 40)
            ts = start
            while ts < end:
                excursion = rng.random() < 0.003
                freq = (
                    NOMINAL_HZ
                    + rng.gauss(0, 0.012)
                    + (rng.choice([-1, 1]) * 0.09 if excursion else 0)
                )
                events.append(
                    {
                        "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                        "substation_id": sub,
                        "ba_code": ba.ba_code,
                        "event_ts": ts.isoformat(),
                        "frequency_hz": round(freq, 4),
                        "load_mw": round(base_load * (1 + rng.gauss(0, 0.05)), 2),
                        "voltage_kv": round(230 * (1 + rng.gauss(0, 0.004)), 2),
                    }
                )
                ts += timedelta(seconds=interval_s)

    # Devices report concurrently, so the stream is ordered by time across substations.
    events.sort(key=lambda e: e["event_ts"])

    # Delivery semantics of a real stream
    delivered: list[dict] = []
    late: list[dict] = []
    for e in events:
        r = rng.random()
        if r < late_rate:
            late.append(e)  # held back by a flaky edge gateway
            continue
        delivered.append(e)
        if r < late_rate + duplicate_rate:
            delivered.append(dict(e))  # redelivery after a missed ack
    # local shuffles -> out-of-order within a small window
    for i in range(0, len(delivered) - 8, 8):
        window = delivered[i : i + 8]
        rng.shuffle(window)
        delivered[i : i + 8] = window
    return delivered + late
