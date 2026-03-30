"""Public API entry points."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pyarrow as pa

from iceberg_bioimage.adapters.base import BaseAdapter
from iceberg_bioimage.adapters.ome_tiff import OMETiffAdapter
from iceberg_bioimage.adapters.zarr_v2 import ZarrV2Adapter
from iceberg_bioimage.integrations.duckdb import (
    MetadataSource,
    join_image_assets_with_profiles,
)
from iceberg_bioimage.models.scan_result import (
    ContractValidationResult,
    DatasetSummary,
    RegistrationResult,
    ScanResult,
)
from iceberg_bioimage.publishing.chunk_index import (
    publish_chunk_index,
    scan_result_to_chunk_rows,
)
from iceberg_bioimage.publishing.image_assets import (
    SupportsCatalog,
    publish_image_assets,
    scan_result_to_rows,
)
from iceberg_bioimage.validation.contracts import (
    validate_microscopy_profile_columns,
    validate_microscopy_profile_table,
)


def _build_adapters() -> Sequence[BaseAdapter]:
    return (ZarrV2Adapter(), OMETiffAdapter())


def scan_store(uri: str) -> ScanResult:
    """Scan a supported image store and return canonical metadata."""

    for adapter in _build_adapters():
        if adapter.can_handle(uri):
            return adapter.scan(uri)

    raise ValueError(
        "Unsupported bioimage URI "
        f"{uri!r}. Supported formats are .zarr, .tif, and .tiff."
    )


def register_store(
    uri: str,
    catalog: str | SupportsCatalog,
    namespace: str | Sequence[str],
    *,
    image_assets_table: str = "image_assets",
    chunk_index_table: str | None = "chunk_index",
) -> RegistrationResult:
    """Scan a store and publish canonical metadata tables."""

    scan_result = scan_store(uri)
    image_assets_rows = publish_image_assets(
        catalog,
        namespace,
        image_assets_table,
        scan_result,
    )
    chunk_rows = 0
    if chunk_index_table is not None:
        chunk_rows = publish_chunk_index(
            catalog,
            namespace,
            chunk_index_table,
            scan_result,
        )

    return RegistrationResult(
        source_uri=scan_result.source_uri,
        image_assets_rows_published=image_assets_rows,
        chunk_rows_published=chunk_rows,
    )


def summarize_scan_result(scan_result: ScanResult) -> DatasetSummary:
    """Build a concise user-facing summary from a scan result."""

    axes = sorted(
        {
            str(asset.metadata["axes"])
            for asset in scan_result.image_assets
            if asset.metadata.get("axes")
        }
    )
    channel_counts = sorted(
        {
            int(asset.metadata["channel_count"])
            for asset in scan_result.image_assets
            if asset.metadata.get("channel_count") is not None
        }
    )
    storage_variants = sorted(
        {
            str(asset.metadata["storage_variant"])
            for asset in scan_result.image_assets
            if asset.metadata.get("storage_variant")
        }
    )
    shapes = sorted({tuple(asset.shape) for asset in scan_result.image_assets})

    return DatasetSummary(
        source_uri=scan_result.source_uri,
        format_family=scan_result.format_family,
        image_asset_count=len(scan_result.image_assets),
        chunked_asset_count=sum(
            1 for asset in scan_result.image_assets if asset.chunk_shape is not None
        ),
        array_paths=[
            asset.array_path if asset.array_path is not None else "<root>"
            for asset in scan_result.image_assets
        ],
        dtypes=sorted({asset.dtype for asset in scan_result.image_assets}),
        shapes=[list(shape) for shape in shapes],
        axes=axes,
        channel_counts=channel_counts,
        storage_variants=storage_variants,
        warnings=list(scan_result.warnings),
    )


def summarize_store(uri: str) -> DatasetSummary:
    """Scan a store and return a concise dataset summary."""

    return summarize_scan_result(scan_store(uri))


def join_profiles_with_scan_result(
    scan_result: ScanResult,
    profiles: MetadataSource,
    *,
    include_chunks: bool = False,
) -> pa.Table:
    """Join canonical image assets from a scan result to profile rows.

    This helper uses the optional DuckDB integration at runtime. Install the
    `duckdb` extra/group before calling it.
    """

    validation = _validate_profiles(profiles)
    if not validation.is_valid:
        raise ValueError(
            "Profiles do not satisfy the microscopy join contract: "
            + ", ".join(validation.missing_required_columns)
        )

    image_assets = pa.Table.from_pylist(scan_result_to_rows(scan_result))
    chunk_index = None
    if include_chunks:
        chunk_index = pa.Table.from_pylist(scan_result_to_chunk_rows(scan_result))

    return join_image_assets_with_profiles(
        image_assets,
        profiles,
        chunk_index=chunk_index,
    )


def join_profiles_with_store(
    uri: str,
    profiles: MetadataSource,
    *,
    include_chunks: bool = False,
) -> pa.Table:
    """Scan a store and join its canonical image assets to profile rows.

    This helper uses the optional DuckDB integration at runtime. Install the
    `duckdb` extra/group before calling it.
    """

    return join_profiles_with_scan_result(
        scan_store(uri),
        profiles,
        include_chunks=include_chunks,
    )


def _validate_profiles(profiles: MetadataSource) -> ContractValidationResult:
    if isinstance(profiles, (str, Path)):
        return validate_microscopy_profile_table(str(profiles))
    if isinstance(profiles, pa.Table):
        return validate_microscopy_profile_columns(list(profiles.schema.names))
    if isinstance(profiles, list):
        columns = sorted({key for row in profiles for key in row})
        return validate_microscopy_profile_columns(columns)

    raise TypeError(f"Unsupported profile source type: {type(profiles)!r}")
