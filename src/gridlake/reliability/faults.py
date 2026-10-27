"""Realistic source faults to inject into a run, so the reliability agent has something to
triage: ``gridlake run --inject-fault <name>``. Each wraps the synthetic EIA source.

* ``new_fuel_type``  EIA starts reporting battery storage (``BAT``) for CISO and ERCO. The
                     rows are valid, but the fuel-type seed does not know the code yet.
* ``unit_change``    ERCO's demand series switches to kWh for the last three days of the
                     window (values 1000x). The data is wrong; the contract is right.
* ``feed_outage``    the region-data endpoint keeps returning HTTP 503 until retries run out.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from gridlake.sources.eia import EIASource, eia_period

FAULTS = ("new_fuel_type", "unit_change", "feed_outage")


class FaultySource:
    def __init__(self, base: EIASource, fault: str):
        if fault not in FAULTS:
            raise ValueError(f"unknown fault {fault!r}; choose from {FAULTS}")
        self.base, self.fault = base, fault
        self.name = f"{base.name}+{fault}"

    def region_data(self, start: datetime, end: datetime) -> list[dict]:
        if self.fault == "feed_outage":
            raise RuntimeError(
                "EIA request failed after 5 attempts: "
                "https://api.eia.gov/v2/electricity/rto/region-data/data/ (HTTP 503)"
            )
        records = self.base.region_data(start, end)
        if self.fault == "unit_change":
            since = eia_period(max(start, end - timedelta(days=3)))
            for r in records:
                if (
                    r["respondent"] == "ERCO"
                    and r["type"] == "D"
                    and r["period"] >= since
                    and r["value"] not in (None, "N/A")
                ):
                    r["value"] = str(abs(int(r["value"])) * 1000)
                    r["value-units"] = "kilowatthours"
        return records

    def fuel_type_data(self, start: datetime, end: datetime) -> list[dict]:
        records = self.base.fuel_type_data(start, end)
        if self.fault == "new_fuel_type":
            seen = {(r["period"], r["respondent"]) for r in records}
            for period, ba in sorted(seen):
                if ba in ("CISO", "ERCO"):
                    hour = int(period[-2:])
                    records.append(
                        {
                            "period": period,
                            "respondent": ba,
                            "respondent-name": ba,
                            "fueltype": "BAT",
                            "type-name": "Battery storage",
                            "value": str(400 + 25 * (hour % 12)),
                            "value-units": "megawatthours",
                        }
                    )
        return records
