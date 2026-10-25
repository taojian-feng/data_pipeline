from datetime import timedelta

from conftest import START
from gridlake.sources.eia import EIAClient, SyntheticEIASource
from gridlake.sources.telemetry import generate_events


def test_synthetic_source_is_deterministic(bas):
    end = START + timedelta(days=1)
    a = SyntheticEIASource(bas).region_data(START, end)
    b = SyntheticEIASource(bas).region_data(START, end)
    assert a == b
    assert len(a) == len(bas) * 24 * 4 + 1  # 4 series per BA-hour + one unknown respondent


def test_synthetic_source_injects_realistic_defects(bas):
    recs = SyntheticEIASource(bas, defect_rate=0.05).region_data(START, START + timedelta(days=2))
    values = [r["value"] for r in recs]
    assert None in values and "N/A" in values
    assert any(v and v.startswith("-") for v in values)
    assert any(r["respondent"] == "XXXX" for r in recs)


def test_fuel_mix_has_solar_only_in_daylight(bas):
    ciso = [b for b in bas if b.ba_code == "CISO"]
    recs = SyntheticEIASource(ciso, defect_rate=0).fuel_type_data(START, START + timedelta(days=1))
    solar = {r["period"]: int(r["value"]) for r in recs if r["fueltype"] == "SUN"}
    assert solar["2026-10-01T10"] == 0  # 03:00 Pacific
    assert solar["2026-10-01T20"] > 0  # 13:00 Pacific


class FakeResponse:
    def __init__(self, status, payload=None):
        self.status_code, self.payload = status, payload

    def json(self):
        return self.payload

    def raise_for_status(self):
        raise RuntimeError(self.status_code)


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []

    def get(self, url, params, timeout):
        self.calls.append(params["offset"])
        return self.responses.pop(0)


def test_eia_client_paginates_and_retries():
    page = lambda rows, total: FakeResponse(200, {"response": {"data": rows, "total": total}})  # noqa: E731
    session = FakeSession(
        [FakeResponse(429), page([{"v": 1}, {"v": 2}], 3), FakeResponse(503), page([{"v": 3}], 3)]
    )
    client = EIAClient("key", ["CISO"], session=session, backoff_s=0)
    client.page_size = 2
    assert client.region_data(START, START + timedelta(hours=3)) == [{"v": 1}, {"v": 2}, {"v": 3}]
    assert session.calls == [0, 0, 2, 2]


def test_telemetry_has_duplicates_and_late_events(bas):
    events = generate_events(bas[:2], START, START + timedelta(hours=1), seed=1)
    ids = [e["event_id"] for e in events]
    assert len(ids) > len(set(ids))  # redeliveries
    ts = [e["event_ts"] for e in events]
    assert ts != sorted(ts)  # out of order
