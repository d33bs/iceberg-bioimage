"""Optional integration helpers."""

from .catalog import (
    CatalogScanOptions,
    catalog_table_to_arrow,
    join_catalog_image_assets_with_profiles,
    list_catalog_tables,
    load_catalog_table,
)
from .duckdb import (
    create_duckdb_connection,
    join_image_assets_with_profiles,
    query_metadata_table,
)
from .ome_arrow import create_ome_arrow, scan_ome_arrow

__all__ = [
    "CatalogScanOptions",
    "catalog_table_to_arrow",
    "create_duckdb_connection",
    "create_ome_arrow",
    "join_catalog_image_assets_with_profiles",
    "join_image_assets_with_profiles",
    "list_catalog_tables",
    "load_catalog_table",
    "query_metadata_table",
    "scan_ome_arrow",
]
