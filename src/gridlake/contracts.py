"""Data contracts: the single source of truth for schema, partitioning, ownership,
classification and quality rules of every table in the lakehouse.

Contracts live as YAML in ``contracts/`` and drive:
  * Iceberg table creation (schema, identifier fields, partition spec)
  * Arrow casting before every write (schema enforcement)
  * Quality gates (``gridlake.quality``)
  * Glue Data Catalog / Lake Formation tags (``infra/terraform`` reads the same files)
  * The knowledge graph and RAG documents that describe the data estate to AI agents
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import cache
from pathlib import Path
from typing import Any

import pyarrow as pa
import yaml
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform, MonthTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    IcebergType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

CLASSIFICATIONS = ("public", "internal", "confidential")

_ICEBERG_TYPES: dict[str, type[IcebergType]] = {
    "string": StringType,
    "double": DoubleType,
    "long": LongType,
    "int": IntegerType,
    "boolean": BooleanType,
    "date": DateType,
    "timestamp": TimestamptzType,
}

_ARROW_TYPES: dict[str, pa.DataType] = {
    "string": pa.large_string(),
    "double": pa.float64(),
    "long": pa.int64(),
    "int": pa.int32(),
    "boolean": pa.bool_(),
    "date": pa.date32(),
    "timestamp": pa.timestamp("us", tz="UTC"),
}

TRANSFORMS = {"day": DayTransform, "month": MonthTransform, "identity": IdentityTransform}


class ContractError(ValueError):
    pass


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True
    description: str = ""
    business_term: str | None = None


@dataclass(frozen=True)
class Contract:
    name: str
    version: str
    layer: str
    domain: str
    owner: str
    classification: str
    description: str
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...] = ()
    partition_by: tuple[dict[str, str], ...] = ()
    quality: tuple[dict[str, Any], ...] = ()
    freshness: dict[str, Any] | None = None
    consumers: tuple[str, ...] = ()
    source: str | None = None
    raw: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @property
    def namespace(self) -> str:
        return self.name.split(".", 1)[0]

    @property
    def table(self) -> str:
        return self.name.split(".", 1)[1]

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def arrow_schema(self) -> pa.Schema:
        return pa.schema(
            [pa.field(c.name, _ARROW_TYPES[c.type], nullable=c.nullable) for c in self.columns]
        )

    def iceberg_schema(self) -> Schema:
        fields = [
            NestedField(
                i,
                c.name,
                _ICEBERG_TYPES[c.type](),
                required=not c.nullable,
                doc=c.description or None,
            )
            for i, c in enumerate(self.columns, start=1)
        ]
        ids = {c.name: i for i, c in enumerate(self.columns, start=1)}
        return Schema(*fields, identifier_field_ids=[ids[k] for k in self.primary_key])

    def partition_spec(self) -> PartitionSpec:
        ids = {c.name: i for i, c in enumerate(self.columns, start=1)}
        fields = []
        for n, p in enumerate(self.partition_by, start=1000):
            transform = TRANSFORMS[p.get("transform", "identity")]()
            fields.append(
                PartitionField(
                    ids[p["column"]],
                    n,
                    transform,
                    f"{p['column']}_{p.get('transform', 'identity')}",
                )
            )
        return PartitionSpec(*fields)

    def conform(self, table: pa.Table) -> pa.Table:
        """Project and cast an Arrow table to the contract schema (schema-on-write).

        Raises ContractError for missing columns or nulls in required columns, so a
        producer can never silently widen or break a published data product.
        """
        missing = [c for c in self.column_names if c not in table.column_names]
        if missing:
            raise ContractError(f"{self.name}: missing columns {missing}")
        arrays = []
        schema = self.arrow_schema()
        for f in schema:
            col = table.column(f.name).cast(f.type)
            if not f.nullable and col.null_count:
                raise ContractError(
                    f"{self.name}: {col.null_count} nulls in required column {f.name!r}"
                )
            arrays.append(col)
        return pa.Table.from_arrays(arrays, schema=schema)


def _parse(raw: dict[str, Any], path: Path) -> Contract:
    for key in ("name", "version", "layer", "owner", "classification", "columns"):
        if key not in raw:
            raise ContractError(f"{path.name}: missing required key {key!r}")
    if raw["classification"] not in CLASSIFICATIONS:
        raise ContractError(f"{path.name}: classification must be one of {CLASSIFICATIONS}")
    columns = tuple(Column(**c) for c in raw["columns"])
    for c in columns:
        if c.type not in _ARROW_TYPES:
            raise ContractError(f"{path.name}: unsupported type {c.type!r} for {c.name}")
    names = {c.name for c in columns}
    for k in raw.get("primary_key", []):
        if k not in names:
            raise ContractError(f"{path.name}: primary key column {k!r} not declared")
    return Contract(
        name=raw["name"],
        version=str(raw["version"]),
        layer=raw["layer"],
        domain=raw.get("domain", ""),
        owner=raw["owner"],
        classification=raw["classification"],
        description=" ".join(str(raw.get("description", "")).split()),
        columns=columns,
        primary_key=tuple(raw.get("primary_key", [])),
        partition_by=tuple(raw.get("partition_by", [])),
        quality=tuple(raw.get("quality", [])),
        freshness=raw.get("freshness"),
        consumers=tuple(raw.get("consumers", [])),
        source=raw.get("source"),
        raw=raw,
    )


@cache
def load_contracts(directory: Path) -> dict[str, Contract]:
    contracts: dict[str, Contract] = {}
    for path in sorted(directory.glob("*.yaml")):
        contract = _parse(yaml.safe_load(path.read_text()), path)
        if path.stem != contract.name:
            raise ContractError(f"{path.name}: file name must match contract name {contract.name}")
        contracts[contract.name] = contract
    return contracts
