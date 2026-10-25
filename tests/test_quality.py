from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest

from gridlake.contracts import Column, Contract
from gridlake.quality.engine import QualityGateError, evaluate, quarantine_table, results_table


def contract(rules, freshness=None):
    return Contract(
        name="silver.t",
        version="1",
        layer="silver",
        domain="d",
        owner="o",
        classification="internal",
        description="",
        primary_key=("id",),
        quality=tuple(rules),
        freshness=freshness,
        columns=(
            Column("id", "long", nullable=False),
            Column("ba", "string"),
            Column("v", "double"),
            Column("ts", "timestamp"),
        ),
    )


def data():
    now = datetime.now(UTC)
    return pa.table(
        {
            "id": [1, 2, 3, 3, None],
            "ba": ["CISO", "XXXX", "ERCO", "ERCO", "PJM"],
            "v": [10.0, -5.0, None, 7.0, 1.0],
            "ts": pa.array(
                [now - timedelta(hours=h) for h in (1, 2, 3, 4, 5)], pa.timestamp("us", tz="UTC")
            ),
        }
    )


def test_row_rules_quarantine_offending_rows():
    rules = [
        {"rule": "range", "column": "v", "min": 0},
        {"rule": "unique", "columns": ["id"]},
        {"rule": "accepted_values", "column": "ba", "values": ["CISO", "ERCO", "PJM"]},
    ]
    report = evaluate(contract(rules), data())
    by_rule = {r.rule.split("(")[0]: r.failed_rows for r in report.results}
    assert by_rule == {"not_null": 1, "range": 1, "unique": 2, "accepted_values": 1}
    assert report.valid.column("id").to_pylist() == [1]
    assert report.quarantined.num_rows == 4
    assert "range(v:0..None)" in report.quarantine_reasons[0]


def test_referential_rule_uses_reference_table():
    rules = [
        {"rule": "referential", "column": "ba", "ref_table": "silver.dim", "ref_column": "code"}
    ]
    ref = pa.table({"code": ["CISO", "ERCO", "PJM"]})
    report = evaluate(contract(rules), data(), reference_loader=lambda name: ref)
    assert [r.failed_rows for r in report.results if r.rule.startswith("referential")] == [1]


def test_expression_rule_treats_null_as_failure():
    report = evaluate(contract([{"rule": "expression", "sql": "v >= 0"}]), data())
    assert [r.failed_rows for r in report.results if r.rule.startswith("expression")] == [2]


def test_warn_rules_never_block():
    report = evaluate(
        contract([{"rule": "range", "column": "v", "min": 0, "severity": "warn"}]), data()
    )
    warn = next(r for r in report.results if r.rule.startswith("range"))
    assert (warn.severity, warn.failed_rows) == ("warn", 1)
    assert report.valid.num_rows == 4  # only the implied not_null(id) quarantines


def test_strict_mode_raises_and_carries_report():
    with pytest.raises(QualityGateError) as exc:
        evaluate(contract([{"rule": "unique", "columns": ["id"]}]), data(), mode="strict")
    assert exc.value.report.table == "silver.t"


def test_dataset_rules_raise_in_quarantine_mode():
    with pytest.raises(QualityGateError, match="row_count_min"):
        evaluate(contract([{"rule": "row_count_min", "min": 10}]), data())


def test_freshness():
    fresh = {"column": "ts", "max_lag_hours": 2, "severity": "error"}
    evaluate(contract([], fresh), data())  # newest row is 1h old
    with pytest.raises(QualityGateError, match="freshness"):
        evaluate(contract([], fresh), data(), now=datetime.now(UTC) + timedelta(hours=5))


def test_result_and_quarantine_tables_shape():
    report = evaluate(contract([{"rule": "range", "column": "v", "min": 0}]), data())
    assert results_table(report, "run").num_rows == len(report.results)
    q = quarantine_table(report, "run")
    assert q.num_rows == report.quarantined.num_rows
    assert any('"ba": "XXXX"' in r for r in q.column("record_json").to_pylist())
