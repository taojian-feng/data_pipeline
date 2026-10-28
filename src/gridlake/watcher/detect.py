"""Find unusual days in the gold data products, before any model is involved.

Detection is plain statistics so it is repeatable and cheap; the model only explains what
this module found. Each balancing authority's day is compared with its own recent history:

* **demand**       average hourly demand (``total_demand_mwh / hours_reported``, so a few
                   missing hours do not read as a demand drop), against the median of earlier
                   days of the same type (weekday or weekend).
* **interchange**  net interchange as a share of demand, in percentage points, against its
                   median. A sign change means the BA went from exporting to importing.
* **fuel share**   each energy source's share of generation, as the change from the day before
                   against the usual day-to-day changes. Wind and solar swing widely over a
                   week, so their level alone says little; an unusually large one-day move
                   does, and so does a big move to well past the window's range (a record low
                   or high). A source with no history that now supplies a real share is new.

A change is flagged only when it is both statistically unusual (a robust z-score from the
median and MAD of the history, at least ``z_min``) and big enough to matter (at least the
signal's minimum move). The z-score scale has a floor, so a very steady history cannot turn a
trivial wobble into an alert. Days with fewer than ``min_hours`` hours reported are not
judged; any day short of 24 hours is listed as a data gap rather than a grid event.
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Any

# signal -> (minimum move, z-score scale floor, unit). Moves are % for demand, pp otherwise.
SIGNALS: dict[str, tuple[float, float, str]] = {
    "demand": (5.0, 1.0, "%"),
    "interchange": (4.0, 1.0, "pp"),
    "fuel_share": (5.0, 1.0, "pp"),
}
NEW_SOURCE_MIN_SHARE = 2.0
SEVERITY_RANK = {"high": 0, "medium": 1}


@dataclass
class Baseline:
    method: str  # median (of earlier days) | previous_day (against usual daily changes)
    days: int
    first_day: str
    last_day: str
    day_type: str = "all"  # weekday, weekend or all


@dataclass
class Anomaly:
    id: str
    date: str
    ba_code: str
    ba_name: str
    signal: str  # demand | interchange | fuel_share | new_source
    metric: str
    value: float
    baseline: float  # the median, or the day before for fuel shares
    change: float  # % for demand, percentage points for the others
    unit: str
    z: float | None
    severity: str  # high | medium
    window: Baseline
    fuel_type: str | None = None
    tables: list[str] = field(default_factory=list)
    note: str | None = None  # e.g. "lowest in 20 days"

    @property
    def direction(self) -> str:
        return "up" if self.change > 0 else "down"

    def label(self) -> str:
        return {
            "demand": "demand",
            "interchange": "net interchange",
            "fuel_share": f"{self.fuel_type} share",
            "new_source": f"new source {self.fuel_type}",
        }[self.signal]

    def headline(self) -> str:
        if self.signal == "new_source":
            return f"{self.ba_code} {self.label()} at {self.value:.1f}% of generation"
        return f"{self.ba_code} {self.label()} {self.change:+.1f}{self.unit}"

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "direction": self.direction, "headline": self.headline()}


@dataclass
class Detection:
    date: str
    lookback_days: int
    balancing_authorities: list[str]
    anomalies: list[Anomaly]
    data_gaps: dict[str, int]  # BA -> hours reported, for days short of 24
    not_judged: dict[str, list[str]]  # signal -> BAs with too little history

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "anomalies": [a.to_dict() for a in self.anomalies]}


def is_weekend(d: date) -> bool:
    return d.weekday() >= 5


def robust_z(value: float, history: list[float], floor: float) -> tuple[float, float]:
    """(median, z) of ``value`` against ``history``, scaled by 1.4826 MAD with a floor."""
    med = statistics.median(history)
    mad = statistics.median(abs(h - med) for h in history)
    return med, (value - med) / max(1.4826 * mad, floor)


def avg_hourly_demand(r: dict) -> float | None:
    if not r.get("total_demand_mwh") or not r.get("hours_reported"):
        return None
    return r["total_demand_mwh"] / r["hours_reported"]


def interchange_pct(r: dict) -> float | None:
    if r.get("net_interchange_mwh") is None or not r.get("total_demand_mwh"):
        return None
    return 100 * r["net_interchange_mwh"] / r["total_demand_mwh"]


def _severity(change: float, minimum: float, z: float | None) -> str:
    return "high" if abs(change) >= 2 * minimum or (z is not None and abs(z) >= 8) else "medium"


def _window(method: str, days: list[date], day_type: str = "all") -> Baseline:
    return Baseline(method, len(days), days[0].isoformat(), days[-1].isoformat(), day_type)


def _anomaly(
    day: date,
    ba: str,
    name: str,
    signal: str,
    metric: str,
    value: float,
    base: float,
    change: float,
    z: float | None,
    window: Baseline,
    fuel: str | None = None,
) -> Anomaly:
    minimum = SIGNALS[signal][0] if signal in SIGNALS else NEW_SOURCE_MIN_SHARE
    return Anomaly(
        id=f"{day.isoformat()}/{ba}/{signal}" + (f"/{fuel}" if fuel else ""),
        date=day.isoformat(),
        ba_code=ba,
        ba_name=name,
        signal=signal,
        metric=metric,
        value=round(value, 2),
        baseline=round(base, 2),
        change=round(change, 2),
        unit=SIGNALS["demand"][2] if signal == "demand" else "pp",
        z=None if z is None else round(z, 1),
        severity="medium" if z is None else _severity(change, minimum, z),
        window=window,
        fuel_type=fuel,
        tables=["gold.fuel_mix_daily" if fuel else "gold.ba_daily_energy"],
    )


def detect(
    daily: list[dict],
    fuel_mix: list[dict],
    day: date,
    lookback: int = 28,
    z_min: float = 3.5,
    min_history: int = 5,
    min_same_type: int = 3,
    min_hours: int = 20,
) -> Detection:
    """Anomalies on ``day`` from ``gold.ba_daily_energy`` and ``gold.fuel_mix_daily`` rows."""
    energy: dict[str, dict[date, dict]] = defaultdict(dict)
    for r in daily:
        energy[r["ba_code"]][r["date"]] = r
    shares: dict[str, dict[str, dict[date, float]]] = defaultdict(lambda: defaultdict(dict))
    for r in fuel_mix:
        shares[r["ba_code"]][r["fuel_type"]][r["date"]] = r["share_pct"]

    out = Detection(day.isoformat(), lookback, [], [], {}, defaultdict(list))
    for ba in sorted(energy):
        rows, today = energy[ba], energy[ba].get(day)
        if today is None:
            continue
        out.balancing_authorities.append(ba)
        hours = today.get("hours_reported") or 0
        if hours < 24:
            out.data_gaps[ba] = hours
        if hours < min_hours:
            continue
        complete = [
            d
            for d in sorted(rows)
            if day - timedelta(days=lookback) <= d < day
            and (rows[d].get("hours_reported") or 0) >= min_hours
        ]
        name = today.get("ba_name") or ba

        # demand: level against the same day type
        same_type = [d for d in complete if is_weekend(d) == is_weekend(day)]
        demand = [(d, v) for d in same_type if (v := avg_hourly_demand(rows[d])) is not None]
        value = avg_hourly_demand(today)
        if value is not None and len(demand) >= min_same_type:
            minimum, floor, _ = SIGNALS["demand"]
            med, z = robust_z(
                value, [v for _, v in demand], floor * statistics.median(v for _, v in demand) / 100
            )
            change = 100 * (value - med) / med
            if abs(z) >= z_min and abs(change) >= minimum:
                window = _window(
                    "median", [d for d, _ in demand], "weekend" if is_weekend(day) else "weekday"
                )
                out.anomalies.append(
                    _anomaly(
                        day,
                        ba,
                        name,
                        "demand",
                        "avg_hourly_demand_mwh",
                        value,
                        med,
                        change,
                        z,
                        window,
                    )
                )
        elif value is not None:
            out.not_judged["demand"].append(ba)

        # interchange: level, as a share of demand
        inter = [(d, v) for d in complete if (v := interchange_pct(rows[d])) is not None]
        value = interchange_pct(today)
        if value is not None and len(inter) >= min_history:
            minimum, floor, _ = SIGNALS["interchange"]
            med, z = robust_z(value, [v for _, v in inter], floor)
            if abs(z) >= z_min and abs(value - med) >= minimum:
                window = _window("median", [d for d, _ in inter])
                out.anomalies.append(
                    _anomaly(
                        day,
                        ba,
                        name,
                        "interchange",
                        "net_interchange_pct_of_demand",
                        value,
                        med,
                        value - med,
                        z,
                        window,
                    )
                )
        elif value is not None:
            out.not_judged["interchange"].append(ba)

        # fuel shares: the one-day move against the usual one-day moves
        prev = day - timedelta(days=1)
        for fuel, series in sorted(shares[ba].items()):
            if day not in series:
                continue
            history = [d for d in complete if d in series]
            if not history:
                if series[day] >= NEW_SOURCE_MIN_SHARE:
                    window = (
                        _window("median", complete) if complete else Baseline("median", 0, "", "")
                    )
                    out.anomalies.append(
                        _anomaly(
                            day,
                            ba,
                            name,
                            "new_source",
                            "share_pct",
                            series[day],
                            0.0,
                            series[day],
                            None,
                            window,
                            fuel,
                        )
                    )
                continue
            moves = [
                (d, series[d] - series[d - timedelta(days=1)])
                for d in history
                if d - timedelta(days=1) in series
            ]
            if prev not in series or len(moves) < min_history:
                out.not_judged["fuel_share"].append(f"{ba}/{fuel}")
                continue
            move = series[day] - series[prev]
            minimum, floor, _ = SIGNALS["fuel_share"]
            _, z = robust_z(move, [m for _, m in moves], floor)
            levels = [series[d] for d in history]
            # A big move to well past anything in the window counts too: a lull that starts
            # from an already low level is a small move by the z-score, but a record low.
            note = None
            if series[day] <= min(levels) - minimum:
                note = f"lowest in {len(levels)} days"
            elif series[day] >= max(levels) + minimum:
                note = f"highest in {len(levels)} days"
            if abs(move) >= minimum and (abs(z) >= z_min or note):
                window = _window("previous_day", [d for d, _ in moves])
                a = _anomaly(
                    day,
                    ba,
                    name,
                    "fuel_share",
                    "share_pct",
                    series[day],
                    series[prev],
                    move,
                    z,
                    window,
                    fuel,
                )
                a.note = note
                out.anomalies.append(a)

    out.not_judged = dict(out.not_judged)
    out.anomalies.sort(key=lambda a: (SEVERITY_RANK[a.severity], -abs(a.z or 0), a.id))
    return out
