"""Public package interface for iceberg_bioimage."""

from .api import (
    join_profiles_with_scan_result,
    join_profiles_with_store,
    register_store,
    scan_store,
    summarize_scan_result,
    summarize_store,
)
from .integrations.catalog import (
    CatalogScanOptions,
    catalog_table_to_arrow,
    join_catalog_image_assets_with_profiles,
    list_catalog_tables,
    load_catalog_table,
)
from .integrations.duckdb import (
    create_duckdb_connection,
    join_image_assets_with_profiles,
    query_metadata_table,
)
from .integrations.ome_arrow import create_ome_arrow, scan_ome_arrow
from .models.scan_result import (
    ContractValidationResult,
    DatasetSummary,
    ImageAsset,
    RegistrationResult,
    ScanResult,
)
from .publishing.chunk_index import publish_chunk_index
from .publishing.image_assets import publish_image_assets
from .validation.contracts import (
    validate_microscopy_profile_columns,
    validate_microscopy_profile_table,
)

__all__ = [
    "CatalogScanOptions",
    "ContractValidationResult",
    "DatasetSummary",
    "ImageAsset",
    "RegistrationResult",
    "ScanResult",
    "catalog_table_to_arrow",
    "create_duckdb_connection",
    "create_ome_arrow",
    "join_catalog_image_assets_with_profiles",
    "join_image_assets_with_profiles",
    "join_profiles_with_scan_result",
    "join_profiles_with_store",
    "list_catalog_tables",
    "load_catalog_table",
    "publish_chunk_index",
    "publish_image_assets",
    "query_metadata_table",
    "register_store",
    "scan_ome_arrow",
    "scan_store",
    "summarize_scan_result",
    "summarize_store",
    "validate_microscopy_profile_columns",
    "validate_microscopy_profile_table",
]
