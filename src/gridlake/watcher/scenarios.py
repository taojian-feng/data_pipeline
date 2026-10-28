"""Real-world grid events to plant in a synthetic run, so the anomaly watcher has something to
find: ``gridlake run --grid-events``. All three land on the last day of the window, and each
wraps any EIA source the way ``reliability.faults`` does.

* ``heatwave``         ERCO demand runs 15% above normal. The day-ahead forecast does not see
                       it coming, gas makes up most of the extra generation and ERCO turns from
                       a small net exporter into a net importer.
* ``wind_lull``        SWPP wind output falls to 15% of normal; coal and gas fill the gap, so
                       the fuel mix and carbon intensity shift while demand does not.
* ``hydro_shortfall``  BPAT hydro output drops by a quarter; BPAT imports the difference.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta

from gridlake.sources.eia import EIASource

EVENTS = ("heatwave", "wind_lull", "hydro_shortfall")

# event -> (ba, {region series: factor}, {fuel: factor})
_EFFECTS: dict[str, tuple[str, dict[str, float], dict[str, float]]] = {
    "heatwave": ("ERCO", {"D": 1.15, "NG": 1.06}, {"NG": 1.13}),
    "wind_lull": ("SWPP", {}, {"WND": 0.15, "COL": 1.45, "NG": 1.6}),
    "hydro_shortfall": ("BPAT", {"NG": 0.8}, {"WAT": 0.75}),
}


def _value(r: dict) -> float | None:
    try:
        return float(r["value"])
    except (TypeError, ValueError):
        return None  # missing or malformed hours stay as they are


class GridEventSource:
    def __init__(self, base: EIASource, events: tuple[str, ...] = EVENTS):
        unknown = sorted(set(events) - set(EVENTS))
        if unknown:
            raise ValueError(f"unknown grid events {unknown}; choose from {EVENTS}")
        self.base, self.events = base, tuple(events)
        self.name = f"{base.name}+events"

    @staticmethod
    def event_day(end: datetime) -> str:
        return (end - timedelta(hours=1)).strftime("%Y-%m-%d")

    def _active(self, end: datetime):
        day = self.event_day(end)
        for event in self.events:
            ba, region, fuel = _EFFECTS[event]
            yield day, ba, region, fuel

    def region_data(self, start: datetime, end: datetime) -> list[dict]:
        records = self.base.region_data(start, end)
        for day, ba, factors, _ in self._active(end):
            if not factors:
                continue
            hours: dict[str, dict[str, dict]] = defaultdict(dict)
            for r in records:
                if r["respondent"] == ba and r["period"].startswith(day):
                    hours[r["period"]][r["type"]] = r
            for series in hours.values():
                shift = 0.0  # interchange = generation - demand, so it absorbs both changes
                for typ, factor in factors.items():
                    r = series.get(typ)
                    v = _value(r) if r else None
                    if v is None:
                        continue
                    r["value"] = str(round(v * factor))
                    shift += (v * factor - v) * (1 if typ == "NG" else -1 if typ == "D" else 0)
                ti = series.get("TI")
                if ti is not None and _value(ti) is not None:
                    ti["value"] = str(round(_value(ti) + shift))
        return records

    def fuel_type_data(self, start: datetime, end: datetime) -> list[dict]:
        records = self.base.fuel_type_data(start, end)
        for day, ba, _, factors in self._active(end):
            for r in records:
                f = factors.get(r["fueltype"])
                v = _value(r)
                if f and v is not None and r["respondent"] == ba and r["period"].startswith(day):
                    r["value"] = str(round(v * f))
        return records
