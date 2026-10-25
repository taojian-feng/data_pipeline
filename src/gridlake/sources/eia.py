"""EIA-930 hourly grid data sources.

``EIAClient`` pulls the real EIA Open Data API v2 (needs a free API key).
``SyntheticEIASource`` produces a deterministic replica with the same JSON shape,
including the defects real feeds have (nulls, malformed values, negative demand,
unknown respondents, restated hours) so the quality gates have something to catch.
Both return records exactly as the API does: a list of dicts with hyphenated keys.
"""

from __future__ import annotations

import csv
import logging
import math
import random
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

import requests

log = logging.getLogger(__name__)

EIA_BASE_URL = "https://api.eia.gov/v2/electricity/rto"
REGION_TYPES = {
    "D": "Demand",
    "DF": "Day-ahead demand forecast",
    "NG": "Net generation",
    "TI": "Total interchange",
}
FUEL_NAMES = {
    "COL": "Coal",
    "NG": "Natural Gas",
    "OIL": "Petroleum",
    "NUC": "Nuclear",
    "SUN": "Solar",
    "WND": "Wind",
    "WAT": "Hydro",
    "OTH": "Other",
}

# Typical generation mix by balancing authority (share of dispatchable/base output).
_MIX = {
    "CISO": {"NG": 0.40, "SUN": 0.25, "WND": 0.08, "WAT": 0.10, "NUC": 0.08, "OTH": 0.09},
    "ERCO": {"NG": 0.45, "WND": 0.25, "SUN": 0.08, "COL": 0.13, "NUC": 0.09},
    "PJM": {"NG": 0.42, "NUC": 0.33, "COL": 0.18, "WND": 0.03, "SUN": 0.02, "WAT": 0.02},
    "MISO": {
        "COL": 0.36,
        "NG": 0.30,
        "WND": 0.15,
        "NUC": 0.14,
        "SUN": 0.02,
        "WAT": 0.01,
        "OTH": 0.02,
    },
    "NYIS": {
        "NG": 0.45,
        "NUC": 0.22,
        "WAT": 0.22,
        "WND": 0.05,
        "SUN": 0.02,
        "OIL": 0.02,
        "OTH": 0.02,
    },
    "ISNE": {
        "NG": 0.52,
        "NUC": 0.24,
        "WAT": 0.07,
        "WND": 0.04,
        "SUN": 0.05,
        "OIL": 0.02,
        "OTH": 0.06,
    },
    "SWPP": {"WND": 0.38, "COL": 0.28, "NG": 0.24, "NUC": 0.05, "WAT": 0.04, "SUN": 0.01},
    "BPAT": {"WAT": 0.82, "WND": 0.09, "NUC": 0.07, "NG": 0.02},
}


def eia_period(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H")


def hours(start: datetime, end: datetime) -> Iterator[datetime]:
    t = start
    while t < end:
        yield t
        t += timedelta(hours=1)


@dataclass(frozen=True)
class BalancingAuthority:
    ba_code: str
    ba_name: str
    region_code: str
    region_name: str
    interconnection: str
    timezone: str
    peak_scale_mw: float
    aliases: str = ""


def load_balancing_authorities(seeds_dir: Path) -> list[BalancingAuthority]:
    with (seeds_dir / "balancing_authorities.csv").open() as f:
        return [
            BalancingAuthority(**{**row, "peak_scale_mw": float(row["peak_scale_mw"])})
            for row in csv.DictReader(f)
        ]


class EIASource(Protocol):
    name: str

    def region_data(self, start: datetime, end: datetime) -> list[dict]: ...

    def fuel_type_data(self, start: datetime, end: datetime) -> list[dict]: ...


class SyntheticEIASource:
    """Deterministic EIA-930 replica. Same (seed, window) always yields the same records."""

    name = "synthetic-eia930"

    def __init__(
        self, bas: Iterable[BalancingAuthority], seed: int = 930, defect_rate: float = 0.004
    ):
        self.bas = list(bas)
        self.seed = seed
        self.defect_rate = defect_rate

    def _demand(self, ba: BalancingAuthority, ts: datetime) -> float:
        local = ts.astimezone(ZoneInfo(ba.timezone))
        rng = random.Random(f"{self.seed}:{ba.ba_code}:{ts.isoformat()}:D")
        daily = (
            0.72
            + 0.20 * math.sin((local.hour - 11) / 24 * 2 * math.pi)
            + 0.06 * math.sin((local.hour - 6) / 12 * 2 * math.pi)
        )
        weekly = 0.92 if local.weekday() >= 5 else 1.0
        return round(ba.peak_scale_mw * daily * weekly * (1 + rng.gauss(0, 0.015)), 0)

    def _solar(self, ba: BalancingAuthority, ts: datetime) -> float:
        h = ts.astimezone(ZoneInfo(ba.timezone)).hour
        return max(0.0, math.sin((h - 6) / 13 * math.pi)) if 6 <= h <= 19 else 0.0

    def _wind(self, ba: BalancingAuthority, ts: datetime) -> float:
        day = ts.timetuple().tm_yday
        return (
            0.55
            + 0.35 * math.sin(day / 3.0 + sum(map(ord, ba.ba_code)) % 7)
            + 0.10 * math.sin(ts.hour / 4)
        )

    def region_data(self, start: datetime, end: datetime) -> list[dict]:
        out: list[dict] = []
        for ba in self.bas:
            for ts in hours(start, end):
                rng = random.Random(f"{self.seed}:{ba.ba_code}:{ts.isoformat()}:R")
                demand = self._demand(ba, ts)
                ng = round(demand * (1 + rng.gauss(0.01, 0.04)), 0)
                values = {
                    "D": demand,
                    "DF": round(demand * (1 + rng.gauss(0, 0.035)), 0),
                    "NG": ng,
                    "TI": ng - demand,
                }
                for typ, value in values.items():
                    out.append(self._record(ts, ba, typ, REGION_TYPES[typ], value, rng, key="type"))
        return out + self._unknown_respondent(start, key="type")

    def fuel_type_data(self, start: datetime, end: datetime) -> list[dict]:
        out: list[dict] = []
        for ba in self.bas:
            mix = _MIX[ba.ba_code]
            for ts in hours(start, end):
                rng = random.Random(f"{self.seed}:{ba.ba_code}:{ts.isoformat()}:F")
                total = self._demand(ba, ts) * 1.01
                weights = {}
                for fuel, share in mix.items():
                    if fuel == "SUN":
                        weights[fuel] = share * 2.2 * self._solar(ba, ts)
                    elif fuel == "WND":
                        weights[fuel] = share * self._wind(ba, ts)
                    else:
                        weights[fuel] = share
                norm = sum(weights.values())
                for fuel, w in weights.items():
                    value = round(total * w / norm * (1 + rng.gauss(0, 0.01)), 0)
                    out.append(
                        self._record(ts, ba, fuel, FUEL_NAMES[fuel], value, rng, key="fueltype")
                    )
        return out

    def _record(self, ts, ba, code, code_name, value, rng: random.Random, key: str) -> dict:
        value_str: str | None = str(int(value))
        roll = rng.random()
        if roll < self.defect_rate:
            value_str = None  # missing hour
        elif roll < self.defect_rate * 1.5:
            value_str = "N/A"  # malformed
        elif roll < self.defect_rate * 2 and code == "D":
            value_str = str(-abs(int(value)))  # sign error from upstream
        return {
            "period": eia_period(ts),
            "respondent": ba.ba_code,
            "respondent-name": ba.ba_name,
            key: code,
            "type-name": code_name,
            "value": value_str,
            "value-units": "megawatthours",
        }

    def _unknown_respondent(self, start: datetime, key: str) -> list[dict]:
        """A respondent that is not in the reference dimension (EIA adds/retires BAs)."""
        return [
            {
                "period": eia_period(start),
                "respondent": "XXXX",
                "respondent-name": "Unregistered BA",
                key: "D",
                "type-name": "Demand",
                "value": "1234",
                "value-units": "megawatthours",
            }
        ]


class EIAClient:
    """Paginated, retrying client for the EIA Open Data API v2."""

    name = "eia-api-v2"
    page_size = 5000

    def __init__(
        self,
        api_key: str,
        respondents: Iterable[str],
        session: requests.Session | None = None,
        max_retries: int = 5,
        backoff_s: float = 1.0,
    ):
        self.api_key = api_key
        self.respondents = list(respondents)
        self.session = session or requests.Session()
        self.max_retries = max_retries
        self.backoff_s = backoff_s

    def _get(self, url: str, params: dict) -> dict:
        for attempt in range(self.max_retries):
            resp = self.session.get(url, params=params, timeout=60)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = self.backoff_s * 2**attempt
                log.warning("EIA %s -> %s, retrying in %.1fs", url, resp.status_code, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
        raise RuntimeError(f"EIA request failed after {self.max_retries} attempts: {url}")

    def _fetch(self, route: str, start: datetime, end: datetime) -> list[dict]:
        url = f"{EIA_BASE_URL}/{route}/data/"
        records: list[dict] = []
        offset = 0
        while True:
            params = {
                "api_key": self.api_key,
                "frequency": "hourly",
                "data[0]": "value",
                "facets[respondent][]": self.respondents,
                "start": eia_period(start),
                "end": eia_period(end - timedelta(hours=1)),
                "sort[0][column]": "period",
                "sort[0][direction]": "asc",
                "offset": offset,
                "length": self.page_size,
            }
            page = self._get(url, params)["response"]
            records.extend(page["data"])
            offset += len(page["data"])
            if not page["data"] or offset >= int(page.get("total", 0)):
                return records

    def region_data(self, start: datetime, end: datetime) -> list[dict]:
        return self._fetch("region-data", start, end)

    def fuel_type_data(self, start: datetime, end: datetime) -> list[dict]:
        return self._fetch("fuel-type-data", start, end)


def utc_day(d: str) -> datetime:
    return datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=UTC)
