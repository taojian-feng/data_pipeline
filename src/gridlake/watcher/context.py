"""The evidence behind each anomaly, gathered from the same gold rows detection used.

``WatchData.context`` answers "what else moved?" for one anomaly: the BA's whole day against
its usual one (demand, peak, forecast error, interchange, generation, renewables, carbon), the
fuels that moved most since the day before, how peer BAs in the same interconnection moved,
and a few plain-language cues (``hints``) that the brief writer turns into an explanation.
Every number comes from ``gold.ba_daily_energy`` or ``gold.fuel_mix_daily``.
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from datetime import date, timedelta
from typing import Any

from gridlake.sources.eia import FUEL_NAMES
from gridlake.watcher.detect import (
    Anomaly,
    Detection,
    avg_hourly_demand,
    interchange_pct,
    is_weekend,
)

TABLES = ["gold.ba_daily_energy", "gold.fuel_mix_daily"]


def _r(v: float | None, n: int = 1) -> float | None:
    return None if v is None else round(v, n)


def _pct(today: float | None, usual: float | None) -> float | None:
    if today is None or not usual:
        return None
    return round(100 * (today - usual) / usual, 1)


def _median(values: list[float | None]) -> float | None:
    vals = [v for v in values if v is not None]
    return statistics.median(vals) if vals else None


class WatchData:
    def __init__(
        self,
        daily: list[dict],
        fuel_mix: list[dict],
        detection: Detection,
        lookback: int = 28,
        min_hours: int = 20,
    ):
        self.detection, self.day = detection, date.fromisoformat(detection.date)
        self.lookback, self.min_hours = lookback, min_hours
        self.energy: dict[str, dict[date, dict]] = defaultdict(dict)
        for r in daily:
            self.energy[r["ba_code"]][r["date"]] = r
        self.fuel: dict[str, dict[date, dict[str, dict]]] = defaultdict(lambda: defaultdict(dict))
        for r in fuel_mix:
            self.fuel[r["ba_code"]][r["date"]][r["fuel_type"]] = r
        self.anomalies = {a.id: a for a in detection.anomalies}

    def _history(self, ba: str, same_type: bool) -> list[dict]:
        rows = self.energy[ba]
        return [
            rows[d]
            for d in sorted(rows)
            if self.day - timedelta(days=self.lookback) <= d < self.day
            and (rows[d].get("hours_reported") or 0) >= self.min_hours
            and (not same_type or is_weekend(d) == is_weekend(self.day))
        ]

    def day_profile(self, ba: str) -> dict[str, Any]:
        today = self.energy[ba][self.day]
        same, every = self._history(ba, same_type=True), self._history(ba, same_type=False)
        same = same if len(same) >= 3 else every

        def usual(rows, fn):
            return _median([fn(r) for r in rows])

        demand, demand_usual = avg_hourly_demand(today), usual(same, avg_hourly_demand)
        gen = lambda r: (r.get("net_generation_mwh") or 0) / (r.get("hours_reported") or 1)  # noqa: E731
        inter, inter_usual = interchange_pct(today), usual(every, interchange_pct)
        col = lambda name: lambda r: r.get(name)  # noqa: E731
        return {
            "hours_reported": today.get("hours_reported"),
            "avg_hourly_demand_mwh": _r(demand, 0),
            "usual_avg_hourly_demand_mwh": _r(demand_usual, 0),
            "demand_change_pct": _pct(demand, demand_usual),
            "peak_demand_mwh": _r(today.get("peak_demand_mwh"), 0),
            "usual_peak_demand_mwh": _r(usual(same, col("peak_demand_mwh")), 0),
            "peak_hour_utc": today.get("peak_hour_utc"),
            "forecast_mape_pct": _r(today.get("forecast_mape_pct"), 2),
            "usual_forecast_mape_pct": _r(usual(same, col("forecast_mape_pct")), 2),
            "net_generation_change_pct": _pct(gen(today), usual(same, gen)),
            "net_interchange_pct_of_demand": _r(inter),
            "usual_net_interchange_pct_of_demand": _r(inter_usual),
            "position": None
            if inter is None
            else ("net exporter" if inter > 0 else "net importer"),
            "usual_position": None
            if inter_usual is None
            else ("net exporter" if inter_usual > 0 else "net importer"),
            "renewable_share_pct": today.get("renewable_share_pct"),
            "usual_renewable_share_pct": _r(usual(every, col("renewable_share_pct")), 2),
            "carbon_intensity_kg_per_mwh": today.get("carbon_intensity_kg_per_mwh"),
            "usual_carbon_intensity_kg_per_mwh": _r(
                usual(every, col("carbon_intensity_kg_per_mwh"))
            ),
        }

    def fuel_moves(self, ba: str, top: int = 4) -> list[dict[str, Any]]:
        today = self.fuel[ba].get(self.day, {})
        before = self.fuel[ba].get(self.day - timedelta(days=1), {})
        moves = []
        for fuel, r in today.items():
            prev = before.get(fuel)
            moves.append(
                {
                    "fuel_type": fuel,
                    "fuel_category": r.get("fuel_category"),
                    "share_pct": r["share_pct"],
                    "share_pct_day_before": prev["share_pct"] if prev else None,
                    "move_pp": _r(r["share_pct"] - prev["share_pct"], 2) if prev else None,
                    "generation_change_pct": _pct(
                        r.get("generation_mwh"), prev.get("generation_mwh") if prev else None
                    ),
                    "generation_change_mwh": _r(r["generation_mwh"] - prev["generation_mwh"], 0)
                    if prev
                    else None,
                }
            )
        moves.sort(key=lambda m: -abs(m["move_pp"] or 0))
        return moves[:top]

    def peers(self, ba: str) -> list[dict[str, Any]]:
        mine = self.energy[ba][self.day].get("interconnection")
        out = []
        for other, rows in sorted(self.energy.items()):
            if other == ba or self.day not in rows or rows[self.day].get("interconnection") != mine:
                continue
            p = self.day_profile(other)
            out.append({"ba_code": other, "demand_change_pct": p["demand_change_pct"]})
        return out

    def context(self, anomaly_id: str) -> dict[str, Any]:
        a = self.anomalies.get(anomaly_id)
        if a is None:
            raise KeyError(f"unknown anomaly {anomaly_id!r}; ids are in the prompt")
        today = self.energy[a.ba_code][self.day]
        profile, moves, peers = (
            self.day_profile(a.ba_code),
            self.fuel_moves(a.ba_code),
            self.peers(a.ba_code),
        )
        return {
            "anomaly": a.to_dict(),
            "balancing_authority": {
                "code": a.ba_code,
                "name": a.ba_name,
                "region": today.get("region_name"),
                "interconnection": today.get("interconnection"),
            },
            "day": profile,
            "fuel_moves_since_day_before": moves,
            "interconnection_peers": peers,
            "related_anomalies": [
                o.id for o in self.detection.anomalies if o.ba_code == a.ba_code and o.id != a.id
            ],
            "hints": hints(a, profile, moves, peers, today.get("interconnection")),
            "tables": TABLES,
        }


def _fuel(code: str | None) -> str:
    return FUEL_NAMES.get(code or "", code or "")


def hints(
    a: Anomaly, day: dict, moves: list[dict], peers: list[dict], interconnection: str | None
) -> list[str]:
    """Plain-language cues read off the evidence. They are observations, not causes."""
    out: list[str] = []
    if (day["hours_reported"] or 24) < 24:
        out.append(
            f"only {day['hours_reported']} of 24 hours were reported, so totals are per reported hour"
        )
    sign = 1 if a.change > 0 else -1
    if a.signal == "demand":
        moved = [p for p in peers if p["demand_change_pct"] is not None]
        with_it = [p for p in moved if sign * p["demand_change_pct"] > 2]
        if not moved:
            out.append(
                f"{a.ba_code} has no peers in the {interconnection} interconnection to compare"
            )
        elif 2 * len(with_it) >= len(moved):
            names = ", ".join(f"{p['ba_code']} {p['demand_change_pct']:+.1f}%" for p in with_it)
            out.append(
                f"peers in the {interconnection} interconnection moved the same way ({names}), "
                "consistent with a regional driver such as weather"
            )
        else:
            out.append(
                f"peers in the {interconnection} interconnection did not move, so the change is local"
            )
        mape, usual = day["forecast_mape_pct"], day["usual_forecast_mape_pct"]
        if mape is not None and usual and mape >= max(2 * usual, 5):
            out.append(
                f"the day-ahead forecast missed by {mape:.1f}% (usually {usual:.1f}%), "
                "so the change was not anticipated"
            )
        elif mape is not None:
            out.append(f"the day-ahead forecast saw it coming (error {mape:.1f}%)")
    switched = (
        day["position"] and day["usual_position"] and day["position"] != day["usual_position"]
    )
    if a.signal in ("demand", "interchange") and switched:
        out.append(f"{a.ba_code} switched from {day['usual_position']} to {day['position']}")
    if a.signal == "interchange":
        dem, gen = day["demand_change_pct"], day["net_generation_change_pct"]
        if dem is not None and gen is not None:
            driver = "generation" if abs(gen) > abs(dem) else "demand"
            out.append(
                f"generation moved {gen:+.1f}% and demand {dem:+.1f}%, so {driver} drove the shift"
            )
        changed = [m for m in moves if abs(m["generation_change_pct"] or 0) >= 5]
        for m in sorted(changed, key=lambda m: -abs(m["generation_change_mwh"] or 0))[:1]:
            verb = "rose" if m["generation_change_pct"] > 0 else "fell"
            out.append(
                f"{_fuel(m['fuel_type'])} output {verb} {abs(m['generation_change_pct']):.1f}% "
                "from the day before"
            )
    if a.signal in ("fuel_share", "new_source"):
        offset = [
            m
            for m in moves
            if m["fuel_type"] != a.fuel_type
            and m["move_pp"] is not None
            and abs(m["move_pp"]) >= 3
            and sign * m["move_pp"] < 0
        ]
        if offset:
            moved = ", ".join(f"{_fuel(m['fuel_type'])} {m['move_pp']:+.1f}pp" for m in offset)
            out.append(f"{_fuel(a.fuel_type)} was offset by {moved}")
    if a.signal in ("fuel_share", "new_source", "interchange"):
        ren, ren_usual = day["renewable_share_pct"], day["usual_renewable_share_pct"]
        ci, ci_usual = day["carbon_intensity_kg_per_mwh"], day["usual_carbon_intensity_kg_per_mwh"]
        if ren is not None and ren_usual is not None and abs(ren - ren_usual) >= 5:
            out.append(f"renewables supplied {ren:.1f}% of generation (usually {ren_usual:.1f}%)")
        if ci is not None and ci_usual and abs(ci - ci_usual) >= max(10, 0.08 * ci_usual):
            out.append(f"carbon intensity was {ci:.0f} kg CO2/MWh (usually {ci_usual:.0f})")
    return out
