import pyarrow as pa
import pytest

from gridlake.contracts import ContractError


def test_all_contracts_load_and_compile(contracts):
    assert len(contracts) >= 14
    for c in contracts.values():
        schema = c.iceberg_schema()
        assert [f.name for f in schema.fields] == c.column_names
        assert {schema.find_field(i).name for i in schema.identifier_field_ids} == set(
            c.primary_key
        )
        c.partition_spec()
        assert c.layer in {"bronze", "silver", "gold", "ops"}


def test_every_gold_product_has_owner_consumers_and_unique_rule(contracts):
    for c in (c for c in contracts.values() if c.layer == "gold"):
        assert c.owner and c.consumers, c.name
        assert any(
            r["rule"] == "unique" and r["columns"] == list(c.primary_key) for r in c.quality
        ), c.name


def test_conform_casts_and_projects(contracts):
    c = contracts["silver.dim_fuel_type"]
    t = pa.table(
        {
            "extra": [1],
            "fuel_type": ["SUN"],
            "fuel_name": ["Solar"],
            "fuel_category": ["Renewable"],
            "is_renewable": [True],
            "emission_factor_kg_per_mwh": [0],
        }
    )
    out = c.conform(t)
    assert out.column_names == c.column_names
    assert out.schema.field("emission_factor_kg_per_mwh").type == pa.float64()


def test_conform_rejects_missing_columns_and_required_nulls(contracts):
    c = contracts["silver.dim_fuel_type"]
    with pytest.raises(ContractError, match="missing columns"):
        c.conform(pa.table({"fuel_type": ["SUN"]}))
    t = pa.table(
        {
            "fuel_type": [None],
            "fuel_name": ["x"],
            "fuel_category": ["x"],
            "is_renewable": [True],
            "emission_factor_kg_per_mwh": [0.0],
        }
    )
    with pytest.raises(ContractError, match="nulls in required column"):
        c.conform(t)
