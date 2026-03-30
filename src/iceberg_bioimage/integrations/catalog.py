"""Catalog-facing helpers for reading canonical Iceberg metadata tables."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa

from iceberg_bioimage.integrations.duckdb import (
    DEFAULT_JOIN_KEYS,
    MetadataSource,
    join_image_assets_with_profiles,
)
from iceberg_bioimage.publishing.image_assets import (
    _normalize_namespace,
    _resolve_catalog,
)


class SupportsIcebergScan(Protocol):
    """Protocol for pyiceberg scan objects."""

    def to_arrow(self) -> pa.Table:
        """Materialize the scan as an Arrow table."""


class SupportsIcebergTable(Protocol):
    """Protocol for pyiceberg table objects."""

    def scan(
        self,
        row_filter: str = "True",
        selected_fields: tuple[str, ...] = ("*",),
        case_sensitive: bool = True,
        snapshot_id: int | None = None,
        limit: int | None = None,
    ) -> SupportsIcebergScan:
        """Return a scan object for the current table."""


class SupportsScanCatalog(Protocol):
    """Protocol for catalogs used by the read-only integration helpers."""

    def load_table(self, identifier: tuple[str, ...]) -> SupportsIcebergTable:
        """Load an existing Iceberg table."""

    def list_tables(self, namespace: tuple[str, ...]) -> list[tuple[str, ...]]:
        """List tables within a namespace."""


@dataclass(frozen=True, slots=True)
class CatalogScanOptions:
    """Options for scanning a catalog-backed metadata table."""

    columns: Sequence[str] | None = None
    where: str | None = None
    snapshot_id: int | None = None
    limit: int | None = None


def load_catalog_table(
    catalog: str | SupportsScanCatalog,
    namespace: str | Sequence[str],
    table_name: str,
) -> SupportsIcebergTable:
    """Load a canonical metadata table from a catalog."""

    resolved_catalog = _resolve_scan_catalog(catalog)
    identifier = (*_normalize_namespace(namespace), table_name)
    return resolved_catalog.load_table(identifier)


def list_catalog_tables(
    catalog: str | SupportsScanCatalog,
    namespace: str | Sequence[str],
) -> list[str]:
    """List canonical metadata tables available in a catalog namespace."""

    resolved_catalog = _resolve_scan_catalog(catalog)
    resolved_namespace = _normalize_namespace(namespace)
    table_names = {
        identifier[-1]
        for identifier in resolved_catalog.list_tables(resolved_namespace)
    }
    return sorted(table_names)


def catalog_table_to_arrow(
    catalog: str | SupportsScanCatalog,
    namespace: str | Sequence[str],
    table_name: str,
    *,
    scan_options: CatalogScanOptions | None = None,
) -> pa.Table:
    """Load a catalog table into Arrow via PyIceberg."""

    options = CatalogScanOptions() if scan_options is None else scan_options
    columns = _normalize_columns(options.columns)
    table = load_catalog_table(catalog, namespace, table_name)
    scan = table.scan(
        row_filter="True" if options.where is None else options.where,
        selected_fields=(("*",) if columns is None else tuple(columns)),
        snapshot_id=options.snapshot_id,
        limit=options.limit,
    )
    return scan.to_arrow()


def join_catalog_image_assets_with_profiles(
    catalog: str | SupportsScanCatalog,
    namespace: str | Sequence[str],
    profiles: MetadataSource,
    *,
    image_assets_table: str = "image_assets",
    chunk_index_table: str | None = None,
    join_keys: Sequence[str] = DEFAULT_JOIN_KEYS,
    image_assets_scan_options: CatalogScanOptions | None = None,
    chunk_index_scan_options: CatalogScanOptions | None = None,
) -> pa.Table:
    """Join catalog-backed image metadata to a profile table.

    Args:
        catalog: Catalog name or catalog-like object.
        namespace: Namespace containing the metadata tables.
        profiles: Profile rows or table to join against.
        image_assets_table: Name of the canonical image-assets table.
        chunk_index_table: Optional chunk-index table name.
        join_keys: Join columns shared by image metadata and profiles.
        image_assets_scan_options: Optional scan options for image-assets reads.
        chunk_index_scan_options: Optional scan options for chunk-index reads.
    """
    normalized_join_keys = _normalize_columns(join_keys)
    if not normalized_join_keys:
        raise ValueError("join_keys must be a non-empty sequence of column names.")
    join_keys = normalized_join_keys

    image_assets = catalog_table_to_arrow(
        catalog,
        namespace,
        image_assets_table,
        scan_options=image_assets_scan_options,
    )
    chunk_index = None
    if chunk_index_table is not None:
        chunk_index = catalog_table_to_arrow(
            catalog,
            namespace,
            chunk_index_table,
            scan_options=chunk_index_scan_options,
        )

    return join_image_assets_with_profiles(
        image_assets,
        profiles,
        join_keys=join_keys,
        chunk_index=chunk_index,
    )


def _normalize_columns(columns: Sequence[str] | None) -> Sequence[str] | None:
    if columns is None:
        return None
    if isinstance(columns, str):
        return [columns]

    return columns


def _resolve_scan_catalog(catalog: str | SupportsScanCatalog) -> SupportsScanCatalog:
    resolved_catalog = _resolve_catalog(catalog)
    if not hasattr(resolved_catalog, "list_tables"):
        raise TypeError("Catalog must provide a list_tables(namespace) method.")

    return resolved_catalog
