"""Tests for gold.ba_weekly_energy, drafted by the gridlake self-service data product agent.

Request:
    Weekly demand and peak per balancing authority for our BI dashboard

Owned by grid-analytics@gridlake.example once merged; edit them like any other test.
"""

from pathlib import Path

import pytest

from gridlake.transform.declared import verify_product

REPO = Path(__file__).resolve().parents[2]
TABLE = "gold.ba_weekly_energy"
COLUMNS = [
    "week_start",
    "ba_code",
    "ba_name",
    "total_demand_mwh",
    "peak_demand_mwh",
    "days_reported",
    "hours_reported",
]
# Each check is a SELECT that returns the rows violating it; no rows means it passes.
CHECKS = {
    "every ba_code and week_start in gold.ba_daily_energy is present": """
        SELECT i.week_start, i.ba_code
        FROM (
            SELECT CAST(date_trunc('week', s.date) AS DATE) AS week_start, s.ba_code AS ba_code,
                   count(*) AS n
            FROM gold.ba_daily_energy s
            GROUP BY 1, 2
        ) i
        LEFT JOIN gold.ba_weekly_energy p USING (week_start, ba_code)
        WHERE p.week_start IS NULL
    """,
    "total_demand_mwh reconciles with gold.ba_daily_energy": """
        SELECT p.week_start, p.ba_code
        FROM gold.ba_weekly_energy p
        JOIN (
            SELECT CAST(date_trunc('week', s.date) AS DATE) AS week_start, s.ba_code AS ba_code,
                   sum(s.total_demand_mwh) AS expected
            FROM gold.ba_daily_energy s
            GROUP BY 1, 2
        ) i USING (week_start, ba_code)
        WHERE abs(coalesce(p.total_demand_mwh, 0) - coalesce(i.expected, 0)) > 0.01
    """,
}


@pytest.fixture(scope="module")
def product(tmp_path_factory):
    return verify_product(TABLE, tmp_path_factory.mktemp("product"), REPO, CHECKS)


def test_meets_the_governance_rules(product):
    assert product.governance == []


def test_builds_rows_with_the_contract_columns(product):
    assert product.rows > 0
    assert product.columns == COLUMNS


def test_passes_its_quality_rules(product):
    assert product.quality_failures == []


@pytest.mark.parametrize("check", sorted(CHECKS))
def test_check(product, check):
    assert product.check_failures[check] == 0


def test_rebuilding_every_period_changes_nothing(product):
    assert product.idempotent
