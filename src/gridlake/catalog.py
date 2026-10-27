"""Iceberg catalog access and contract-driven table management.

* ``local``    - SQLite-backed Iceberg catalog + local filesystem warehouse (no cloud needed)
* ``glue``     - AWS Glue Data Catalog with an S3 warehouse (Lake Formation governed)
* ``s3tables`` - Amazon S3 Tables via its Iceberg REST endpoint (SigV4 signed)

The pipeline code is identical across all three; only the catalog changes.
"""

from __future__ import annotations

import warnings

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import BooleanExpression
from pyiceberg.table import Table

from gridlake.config import Settings
from gridlake.contracts import TRANSFORMS, Contract

OLD = "__old"  # suffix for a column being swapped out by Lakehouse._reconcile_types


def get_catalog(settings: Settings) -> Catalog:
    if settings.catalog_type == "local":
        settings.ensure_dirs()
        return load_catalog(
            "gridlake",
            type="sql",
            uri=f"sqlite:///{settings.home / 'catalog.db'}",
            warehouse=settings.warehouse_dir.as_uri(),
        )
    if settings.catalog_type == "glue":
        props = {"type": "glue", "glue.region": settings.aws_region}
        if settings.glue_warehouse:
            props["warehouse"] = settings.glue_warehouse
        return load_catalog("gridlake", **props)
    if settings.catalog_type == "s3tables":
        if not settings.s3_tables_bucket_arn:
            raise ValueError("GRIDLAKE_S3_TABLES_ARN must be set for the s3tables catalog")
        return load_catalog(
            "gridlake",
            type="rest",
            uri=f"https://s3tables.{settings.aws_region}.amazonaws.com/iceberg",
            warehouse=settings.s3_tables_bucket_arn,
            **{
                "rest.sigv4-enabled": "true",
                "rest.signing-name": "s3tables",
                "rest.signing-region": settings.aws_region,
            },
        )
    raise ValueError(f"unknown catalog type {settings.catalog_type!r}")


class Lakehouse:
    """Thin facade binding contracts to Iceberg tables."""

    def __init__(self, catalog: Catalog, contracts: dict[str, Contract]):
        self.catalog = catalog
        self.contracts = contracts

    def contract(self, name: str) -> Contract:
        return self.contracts[name]

    def table(self, name: str) -> Table:
        try:
            table = self._reconcile_types(self.catalog.load_table(name))
            return self._evolve_partitioning(self._add_new_columns(table))
        except NoSuchTableError:
            c = self.contracts[name]
            self.catalog.create_namespace_if_not_exists(c.namespace)
            return self.catalog.create_table(
                name,
                schema=c.iceberg_schema(),
                partition_spec=c.partition_spec(),
                properties={
                    "format-version": "2",
                    "write.parquet.compression-codec": "zstd",
                    "gridlake.owner": c.owner,
                    "gridlake.classification": c.classification,
                    "gridlake.contract-version": c.version,
                    "gridlake.layer": c.layer,
                },
            )

    def _reconcile_types(self, table: Table) -> Table:
        """Glue's Iceberg CREATE (Terraform) only knows zone-less ``timestamp``. While a
        pre-created table is still empty, swap any column whose type differs from the contract
        for one of the contract type, keeping column order. Nothing is rewritten: no data yet.
        All changes go in one transaction, and leftovers of an interrupted swap are dropped."""
        name = ".".join(table.name()[-2:])
        c = self.contracts.get(name)
        if c is None or table.current_snapshot() is not None:
            return table
        want = {f.name: f.field_type for f in c.iceberg_schema().fields}
        have = table.schema()
        drift = [f.name for f in have.fields if f.name in want and f.field_type != want[f.name]]
        leftover = [
            f.name for f in have.fields if f.name.endswith(OLD) and f.name[: -len(OLD)] in want
        ]
        if not drift and not leftover:
            return table
        doomed = {have.find_field(col).field_id for col in drift + leftover}
        stale = [p.name for p in table.spec().fields if p.source_id in doomed]
        with table.transaction() as tx:
            if stale:  # _evolve_partitioning re-adds these on the swapped columns
                with tx.update_spec() as spec:
                    for p in stale:
                        spec.remove_field(p)
            if leftover:
                with tx.update_schema() as update:
                    for col in leftover:
                        update.delete_column(col)
            if drift:
                with tx.update_schema() as update:
                    for col in drift:
                        update.rename_column(col, f"{col}{OLD}")
                with tx.update_schema() as update:
                    for col in drift:
                        update.add_column(col, want[col], doc=have.find_field(col).doc)
                        update.move_after(col, f"{col}{OLD}")
                with tx.update_schema() as update:
                    for col in drift:
                        update.delete_column(f"{col}{OLD}")
        return self.catalog.load_table(name)

    def _add_new_columns(self, table: Table) -> Table:
        """Additive schema evolution: a contract minor version may add optional columns, and
        tables created before it (locally, or by Terraform, which leaves the schema to Iceberg
        after CREATE) gain them in contract order. Metadata-only; old rows read as null."""
        c = self.contracts.get(".".join(table.name()[-2:]))
        have = {f.name for f in table.schema().fields}
        new = [col for col in c.columns if col.name not in have] if c else []
        if not new:
            return table
        if any(not col.nullable for col in new):
            raise ValueError(f"{c.name}: columns added to an existing table must be nullable")
        names = c.column_names
        with table.update_schema() as update:
            for col in new:
                update.add_column(
                    col.name,
                    c.iceberg_schema().find_field(col.name).field_type,
                    doc=col.description or None,
                )
                before = names[names.index(col.name) - 1] if names.index(col.name) else None
                if before is None:
                    update.move_first(col.name)
                else:
                    update.move_after(col.name, before)
        return self.catalog.load_table(c.name)

    def _evolve_partitioning(self, table: Table) -> Table:
        """Apply the contract's partition spec to tables pre-created unpartitioned (e.g. by
        Terraform from the same contracts). Iceberg partition evolution is metadata-only."""
        c = self.contracts.get(".".join(table.name()[-2:]))
        if c is None or not c.partition_by or table.spec().fields:
            return table
        with table.update_spec() as spec:
            for p in c.partition_by:
                transform = p.get("transform", "identity")
                spec.add_field(p["column"], TRANSFORMS[transform](), f"{p['column']}_{transform}")
        return self.catalog.load_table(".".join(table.name()[-2:]))

    def read(self, name: str, row_filter: BooleanExpression | str | None = None) -> pa.Table:
        table = self.table(name)
        scan = table.scan(row_filter=row_filter) if row_filter is not None else table.scan()
        return scan.to_arrow()

    def append(self, name: str, data: pa.Table) -> int:
        data = self.contracts[name].conform(data)
        if data.num_rows:
            self.table(name).append(data)
        return data.num_rows

    def upsert(self, name: str, data: pa.Table) -> tuple[int, int]:
        """Idempotent MERGE on the contract primary key. Returns (updated, inserted)."""
        c = self.contracts[name]
        data = c.conform(data)
        if not data.num_rows:
            return (0, 0)
        result = self.table(name).upsert(data, join_cols=list(c.primary_key))
        return (result.rows_updated, result.rows_inserted)

    def overwrite(
        self, name: str, data: pa.Table, overwrite_filter: BooleanExpression | None = None
    ) -> int:
        """Replace rows matching ``overwrite_filter`` (dynamic partition overwrite) or the whole table."""
        data = self.contracts[name].conform(data)
        table = self.table(name)
        if overwrite_filter is None:
            table.overwrite(data)
        else:
            with warnings.catch_warnings():  # first load: "delete did not match any records"
                warnings.filterwarnings("ignore", message="Delete operation did not match")
                table.overwrite(data, overwrite_filter=overwrite_filter)
        return data.num_rows

    def snapshot_id(self, name: str) -> int | None:
        snap = self.table(name).current_snapshot()
        return snap.snapshot_id if snap else None
