"""Helpers for exporting Parquet-backed Cytomining warehouse layouts."""

from __future__ import annotations

import json
import logging
import re
import shutil
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from iceberg_bioimage.integrations.catalog import SupportsScanCatalog
from iceberg_bioimage.integrations.duckdb import MetadataSource
from iceberg_bioimage.models.scan_result import (
    CytominingWarehouseResult,
    ScanResult,
    WarehouseManifest,
    WarehouseTableManifestEntry,
)
from iceberg_bioimage.publishing.chunk_index import scan_result_to_chunk_rows
from iceberg_bioimage.publishing.image_assets import scan_result_to_rows
from iceberg_bioimage.validation.contracts import resolve_microscopy_profile_columns

WriteMode = Literal["overwrite", "append"]
DEFAULT_PROFILE_NAMESPACE = "profiles"
DEFAULT_IMAGE_NAMESPACE = "images"
DEFAULT_QUALITY_CONTROL_NAMESPACE = "quality_control"
DEFAULT_JOINED_PROFILES_TABLE = f"{DEFAULT_PROFILE_NAMESPACE}.joined_profiles"
DEFAULT_IMAGE_ASSETS_TABLE = f"{DEFAULT_IMAGE_NAMESPACE}.image_assets"
DEFAULT_CHUNK_INDEX_TABLE = f"{DEFAULT_IMAGE_NAMESPACE}.chunk_index"
DEFAULT_WAREHOUSE_SPEC_VERSION = "1.0.0"
_TABLE_NAME_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
logger = logging.getLogger(__name__)


def export_scan_result_to_cytomining_warehouse(  # noqa: PLR0913
    scan_result: ScanResult,
    warehouse_root: str | Path,
    *,
    profiles: MetadataSource | None = None,
    include_chunks: bool = True,
    image_assets_table_name: str = DEFAULT_IMAGE_ASSETS_TABLE,
    chunk_index_table_name: str = DEFAULT_CHUNK_INDEX_TABLE,
    joined_table_name: str = DEFAULT_JOINED_PROFILES_TABLE,
    profile_dataset_id: str | None = None,
    mode: WriteMode = "overwrite",
) -> CytominingWarehouseResult:
    """Write scan-derived metadata into a Parquet-backed Cytomining warehouse."""

    from iceberg_bioimage.api import join_profiles_with_scan_result

    root = Path(warehouse_root)
    image_assets = pa.Table.from_pylist(scan_result_to_rows(scan_result))
    row_counts: dict[str, int] = {}
    tables_written: list[str] = []

    image_assets_result = export_table_to_cytomining_warehouse(
        image_assets,
        root,
        table_name=image_assets_table_name,
        role="image_assets",
        join_keys=["dataset_id", "image_id"],
        source_type="scan_result",
        source_ref=scan_result.source_uri,
        mode=mode,
        default_namespace=DEFAULT_IMAGE_NAMESPACE,
    )
    tables_written.extend(image_assets_result.tables_written)
    row_counts.update(image_assets_result.row_counts)
    manifest_path = image_assets_result.manifest_path

    if include_chunks:
        chunk_index = pa.Table.from_pylist(scan_result_to_chunk_rows(scan_result))
        chunk_result = export_table_to_cytomining_warehouse(
            chunk_index,
            root,
            table_name=chunk_index_table_name,
            role="chunk_index",
            join_keys=["dataset_id", "image_id", "array_path"],
            source_type="scan_result",
            source_ref=scan_result.source_uri,
            mode=mode,
            default_namespace=DEFAULT_IMAGE_NAMESPACE,
        )
        tables_written.extend(chunk_result.tables_written)
        row_counts.update(chunk_result.row_counts)
        manifest_path = chunk_result.manifest_path

    if profiles is not None:
        joined_profiles = join_profiles_with_scan_result(
            scan_result,
            profiles,
            include_chunks=include_chunks,
            profile_dataset_id=profile_dataset_id,
        )
        joined_result = export_table_to_cytomining_warehouse(
            joined_profiles,
            root,
            table_name=joined_table_name,
            role="joined_profiles",
            join_keys=["dataset_id", "image_id"],
            source_type="joined_profiles",
            source_ref=scan_result.source_uri,
            mode=mode,
            default_namespace=DEFAULT_PROFILE_NAMESPACE,
        )
        tables_written.extend(joined_result.tables_written)
        row_counts.update(joined_result.row_counts)
        manifest_path = joined_result.manifest_path

    return CytominingWarehouseResult(
        warehouse_root=str(root),
        tables_written=tables_written,
        row_counts=row_counts,
        manifest_path=manifest_path,
    )


def export_store_to_cytomining_warehouse(  # noqa: PLR0913
    uri: str,
    warehouse_root: str | Path,
    *,
    profiles: MetadataSource | None = None,
    include_chunks: bool = True,
    image_assets_table_name: str = DEFAULT_IMAGE_ASSETS_TABLE,
    chunk_index_table_name: str = DEFAULT_CHUNK_INDEX_TABLE,
    joined_table_name: str = DEFAULT_JOINED_PROFILES_TABLE,
    profile_dataset_id: str | None = None,
    mode: WriteMode = "overwrite",
) -> CytominingWarehouseResult:
    """Scan a store and export its metadata into a Cytomining warehouse."""

    from iceberg_bioimage.api import scan_store

    return export_scan_result_to_cytomining_warehouse(
        scan_store(uri),
        warehouse_root,
        profiles=profiles,
        include_chunks=include_chunks,
        image_assets_table_name=image_assets_table_name,
        chunk_index_table_name=chunk_index_table_name,
        joined_table_name=joined_table_name,
        profile_dataset_id=profile_dataset_id,
        mode=mode,
    )


def export_catalog_to_cytomining_warehouse(  # noqa: PLR0913
    catalog: str | SupportsScanCatalog,
    namespace: str | tuple[str, ...],
    warehouse_root: str | Path,
    *,
    profiles: MetadataSource | None = None,
    image_assets_table_name: str = DEFAULT_IMAGE_ASSETS_TABLE,
    chunk_index_table_name: str | None = DEFAULT_CHUNK_INDEX_TABLE,
    joined_table_name: str = DEFAULT_JOINED_PROFILES_TABLE,
    catalog_image_assets_table_name: str | None = None,
    catalog_chunk_index_table_name: str | None = None,
    profile_dataset_id: str | None = None,
    mode: WriteMode = "overwrite",
) -> CytominingWarehouseResult:
    """Materialize catalog-backed metadata into a Parquet Cytomining warehouse."""

    from iceberg_bioimage.integrations.catalog import (
        catalog_table_to_arrow,
        join_catalog_image_assets_with_profiles,
    )

    root = Path(warehouse_root)
    row_counts: dict[str, int] = {}
    tables_written: list[str] = []
    resolved_catalog_image_assets_table = (
        _catalog_table_leaf_name(image_assets_table_name)
        if catalog_image_assets_table_name is None
        else _catalog_table_leaf_name(catalog_image_assets_table_name)
    )
    resolved_catalog_chunk_index_table = None
    if chunk_index_table_name is not None:
        resolved_catalog_chunk_index_table = (
            _catalog_table_leaf_name(chunk_index_table_name)
            if catalog_chunk_index_table_name is None
            else _catalog_table_leaf_name(catalog_chunk_index_table_name)
        )

    image_assets = catalog_table_to_arrow(
        catalog,
        namespace,
        resolved_catalog_image_assets_table,
    )
    image_assets_result = export_table_to_cytomining_warehouse(
        image_assets,
        root,
        table_name=image_assets_table_name,
        role="image_assets",
        join_keys=["dataset_id", "image_id"],
        source_type="catalog",
        source_ref=_catalog_source_ref(
            catalog,
            namespace,
            resolved_catalog_image_assets_table,
        ),
        mode=mode,
        default_namespace=DEFAULT_IMAGE_NAMESPACE,
    )
    tables_written.extend(image_assets_result.tables_written)
    row_counts.update(image_assets_result.row_counts)
    manifest_path = image_assets_result.manifest_path

    if (
        chunk_index_table_name is not None
        and resolved_catalog_chunk_index_table is not None
    ):
        chunk_index = catalog_table_to_arrow(
            catalog,
            namespace,
            resolved_catalog_chunk_index_table,
        )
        chunk_result = export_table_to_cytomining_warehouse(
            chunk_index,
            root,
            table_name=chunk_index_table_name,
            role="chunk_index",
            join_keys=["dataset_id", "image_id", "array_path"],
            source_type="catalog",
            source_ref=_catalog_source_ref(
                catalog,
                namespace,
                resolved_catalog_chunk_index_table,
            ),
            mode=mode,
            default_namespace=DEFAULT_IMAGE_NAMESPACE,
        )
        tables_written.extend(chunk_result.tables_written)
        row_counts.update(chunk_result.row_counts)
        manifest_path = chunk_result.manifest_path

    if profiles is not None:
        joined_profiles = join_catalog_image_assets_with_profiles(
            catalog,
            namespace,
            profiles,
            image_assets_table=resolved_catalog_image_assets_table,
            chunk_index_table=resolved_catalog_chunk_index_table,
            profile_dataset_id=profile_dataset_id,
        )
        joined_result = export_table_to_cytomining_warehouse(
            joined_profiles,
            root,
            table_name=joined_table_name,
            role="joined_profiles",
            join_keys=["dataset_id", "image_id"],
            source_type="catalog_join",
            source_ref=_catalog_source_ref(catalog, namespace, joined_table_name),
            mode=mode,
            default_namespace=DEFAULT_PROFILE_NAMESPACE,
        )
        tables_written.extend(joined_result.tables_written)
        row_counts.update(joined_result.row_counts)
        manifest_path = joined_result.manifest_path

    return CytominingWarehouseResult(
        warehouse_root=str(root),
        tables_written=tables_written,
        row_counts=row_counts,
        manifest_path=manifest_path,
    )


def export_profiles_to_cytomining_warehouse(  # noqa: PLR0913
    profiles: MetadataSource,
    warehouse_root: str | Path,
    *,
    table_name: str = f"{DEFAULT_PROFILE_NAMESPACE}.profiles",
    role: str = "profiles",
    profile_dataset_id: str | None = None,
    join_keys: list[str] | None = None,
    source_type: str = "profiles",
    source_ref: str | None = None,
    alias_map: Mapping[str, tuple[str, ...] | list[str]] | None = None,
    mode: WriteMode = "append",
) -> CytominingWarehouseResult:
    """Write a Cytomining profile table into a Parquet-backed warehouse root."""

    root = Path(warehouse_root)
    table = _normalize_profiles_table(
        _metadata_source_to_table(profiles),
        profile_dataset_id=profile_dataset_id,
        alias_map=alias_map,
    )
    return export_table_to_cytomining_warehouse(
        table,
        root,
        table_name=table_name,
        role=role,
        join_keys=[] if join_keys is None else join_keys,
        source_type=source_type,
        source_ref=source_ref if source_ref is not None else str(profiles),
        mode=mode,
        default_namespace=_default_namespace_for_role(role),
    )


def export_table_to_cytomining_warehouse(  # noqa: PLR0913
    table: pa.Table,
    warehouse_root: str | Path,
    *,
    table_name: str,
    role: str,
    join_keys: list[str] | None = None,
    source_type: str | None = None,
    source_ref: str | None = None,
    mode: WriteMode = "append",
    default_namespace: str | None = None,
) -> CytominingWarehouseResult:
    """Write a generic table into a warehouse root and update the manifest."""

    root = Path(warehouse_root)
    normalized_table_name, dataset_path = _resolve_table_layout(
        root,
        table_name,
        default_namespace=default_namespace,
    )
    _validate_role_namespace(normalized_table_name, role)
    _write_parquet_dataset(
        table,
        dataset_path,
        mode=mode,
    )
    manifest_path = _update_manifest(
        root,
        WarehouseTableManifestEntry(
            table_name=normalized_table_name,
            role=role,
            join_keys=[] if join_keys is None else join_keys,
            source_type=source_type,
            source_ref=source_ref,
            row_count=table.num_rows,
            columns=list(table.schema.names),
        ),
    )
    return CytominingWarehouseResult(
        warehouse_root=str(root),
        tables_written=[normalized_table_name],
        row_counts={normalized_table_name: table.num_rows},
        manifest_path=str(manifest_path),
    )


def _resolve_table_layout(
    warehouse_root: Path,
    table_name: str,
    *,
    default_namespace: str | None = None,
) -> tuple[str, Path]:
    normalized_name, parts = _normalize_table_identifier(
        table_name,
        default_namespace=default_namespace,
    )
    return normalized_name, warehouse_root.joinpath(*parts)


def _normalize_table_identifier(
    table_name: str,
    *,
    default_namespace: str | None = None,
) -> tuple[str, tuple[str, ...]]:
    normalized = table_name.strip()
    if not normalized:
        raise ValueError("table_name must not be empty.")

    parts = [part.strip() for part in normalized.split(".")]
    if any(part == "" for part in parts):
        raise ValueError("malformed table_name: empty segment")

    if "." not in normalized and default_namespace is not None:
        namespace = default_namespace.strip()
        if namespace:
            namespace_parts = [part.strip() for part in namespace.split(".")]
            if any(part == "" for part in namespace_parts):
                raise ValueError("malformed default_namespace: empty segment")
            parts = [*namespace_parts, *parts]

    illegal = next(
        (part for part in parts if _TABLE_NAME_SEGMENT_PATTERN.fullmatch(part) is None),
        None,
    )
    if illegal is not None:
        raise ValueError(f"malformed table_name: illegal segment {illegal!r}")

    return ".".join(parts), tuple(parts)


def _default_namespace_for_role(role: str) -> str:
    if role == "quality_control":
        return DEFAULT_QUALITY_CONTROL_NAMESPACE
    return DEFAULT_PROFILE_NAMESPACE


def _validate_role_namespace(table_name: str, role: str) -> None:
    if role != "quality_control":
        return
    namespace = table_name.split(".", maxsplit=1)[0]
    if namespace != DEFAULT_QUALITY_CONTROL_NAMESPACE:
        raise ValueError("quality_control role must use the quality_control namespace.")


def _write_parquet_dataset(
    table: pa.Table,
    dataset_path: Path,
    *,
    mode: WriteMode,
) -> None:
    if mode not in {"overwrite", "append"}:
        raise ValueError("mode must be either 'overwrite' or 'append'.")

    if mode == "overwrite" and dataset_path.exists():
        shutil.rmtree(dataset_path)

    dataset_path.mkdir(parents=True, exist_ok=True)
    file_path = dataset_path / f"part-{uuid.uuid4().hex}.parquet"
    pq.write_table(table, file_path)


def load_warehouse_manifest(warehouse_root: str | Path) -> WarehouseManifest:
    """Load a warehouse manifest if present, otherwise return an empty manifest."""

    root = Path(warehouse_root)
    manifest_path = root / "warehouse_manifest.json"
    if not manifest_path.exists():
        return WarehouseManifest(warehouse_root=str(root))

    payload = json.loads(manifest_path.read_text())
    return WarehouseManifest(
        warehouse_root=payload["warehouse_root"],
        warehouse_spec_version=payload.get("warehouse_spec_version"),
        tables=[
            WarehouseTableManifestEntry(
                table_name=table["table_name"],
                role=table["role"],
                format=table.get("format", "parquet"),
                join_keys=list(table.get("join_keys", [])),
                source_type=table.get("source_type"),
                source_ref=table.get("source_ref"),
                row_count=table.get("row_count"),
                columns=list(table.get("columns", [])),
            )
            for table in payload.get("tables", [])
        ],
    )


def _update_manifest(
    warehouse_root: Path,
    entry: WarehouseTableManifestEntry,
) -> Path:
    manifest = load_warehouse_manifest(warehouse_root)
    manifest.warehouse_root = str(warehouse_root)
    manifest.tables = [
        _normalize_legacy_manifest_entry(table) for table in manifest.tables
    ]
    if manifest.warehouse_spec_version != DEFAULT_WAREHOUSE_SPEC_VERSION:
        if manifest.warehouse_spec_version is not None:
            logger.warning(
                "Normalizing warehouse_spec_version from %s to %s",
                manifest.warehouse_spec_version,
                DEFAULT_WAREHOUSE_SPEC_VERSION,
            )
        manifest.warehouse_spec_version = DEFAULT_WAREHOUSE_SPEC_VERSION
    manifest.tables = [
        table for table in manifest.tables if table.table_name != entry.table_name
    ]
    manifest.tables.append(entry)
    manifest_path = warehouse_root / "warehouse_manifest.json"
    warehouse_root.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(manifest.to_json(indent=2, sort_keys=True))
    return manifest_path


def _metadata_source_to_table(source: MetadataSource) -> pa.Table:
    if isinstance(source, pa.Table):
        return source
    if isinstance(source, list):
        return pa.Table.from_pylist(source)
    if isinstance(source, (str, Path)):
        return ds.dataset(source).to_table()

    raise TypeError(f"Unsupported metadata source type: {type(source)!r}")


def _normalize_profiles_table(
    table: pa.Table,
    *,
    profile_dataset_id: str | None,
    alias_map: Mapping[str, tuple[str, ...] | list[str]] | None = None,
) -> pa.Table:
    resolved_columns = resolve_microscopy_profile_columns(
        list(table.schema.names),
        alias_map=alias_map,
    )
    normalized = table

    for canonical in ("dataset_id", "image_id", "plate_id", "well_id", "site_id"):
        if canonical in normalized.schema.names:
            continue

        source = resolved_columns[canonical]
        if source is not None:
            normalized = normalized.append_column(
                canonical,
                normalized[source],
            )
            continue

        if canonical == "dataset_id" and profile_dataset_id is not None:
            normalized = normalized.append_column(
                canonical,
                pa.repeat(pa.scalar(profile_dataset_id), normalized.num_rows),
            )

    return normalized


def _catalog_source_ref(
    catalog: str | SupportsScanCatalog,
    namespace: str | tuple[str, ...],
    table_name: str,
) -> str:
    namespace_label = namespace if isinstance(namespace, str) else ".".join(namespace)
    catalog_label = catalog if isinstance(catalog, str) else type(catalog).__name__
    return f"{catalog_label}:{namespace_label}.{table_name}"


def _catalog_table_leaf_name(table_identifier: str) -> str:
    try:
        normalized_identifier, _ = _normalize_table_identifier(table_identifier)
    except ValueError as exc:
        if "empty segment" in str(exc):
            raise ValueError(
                "malformed catalog table identifier: empty leaf segment"
            ) from exc
        if "illegal segment" in str(exc):
            illegal = str(exc).split("illegal segment", maxsplit=1)[-1].strip()
            raise ValueError(
                f"malformed catalog table identifier: illegal leaf segment {illegal}"
            ) from exc
        raise ValueError(f"malformed catalog table identifier: {exc}") from exc

    leaf = normalized_identifier.rsplit(".", maxsplit=1)[-1].strip()
    if not leaf:
        raise ValueError("malformed catalog table identifier: empty leaf segment")
    if "." in leaf:
        raise ValueError(
            "malformed catalog table identifier: leaf must not contain '.'"
        )
    if _TABLE_NAME_SEGMENT_PATTERN.fullmatch(leaf) is None:
        raise ValueError(
            f"malformed catalog table identifier: illegal leaf segment {leaf!r}"
        )

    return leaf


def _normalize_legacy_manifest_entry(
    table: WarehouseTableManifestEntry,
) -> WarehouseTableManifestEntry:
    if "." in table.table_name:
        return table

    legacy_table_map = {
        "image_assets": "images.image_assets",
        "chunk_index": "images.chunk_index",
        "joined_profiles": "profiles.joined_profiles",
        "image_crops": "images.image_crops",
        "source_images": "images.source_images",
        "profile_with_images": "profiles.profile_with_images",
    }
    normalized_name = legacy_table_map.get(table.table_name)
    if normalized_name is None:
        fallback_leaf = re.sub(r"[^A-Za-z0-9_-]+", "_", table.table_name.strip())
        fallback_leaf = fallback_leaf.strip("_")
        if not fallback_leaf:
            fallback_leaf = "unknown_table"
        normalized_name = f"legacy.{fallback_leaf}"
        logger.warning(
            "Unknown legacy manifest table_name %s while normalizing to %s; "
            "falling back to %s",
            table.table_name,
            DEFAULT_WAREHOUSE_SPEC_VERSION,
            normalized_name,
        )
    else:
        logger.warning(
            "Normalizing legacy manifest table_name from %s to %s",
            table.table_name,
            normalized_name,
        )
    return WarehouseTableManifestEntry(
        table_name=normalized_name,
        role=table.role,
        format=table.format,
        join_keys=list(table.join_keys),
        source_type=table.source_type,
        source_ref=table.source_ref,
        row_count=table.row_count,
        columns=list(table.columns),
    )
