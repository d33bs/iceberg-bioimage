"""Validation helpers for canonical scan objects and join contracts."""

from __future__ import annotations

import tomllib
from collections.abc import Callable, Mapping
from json import JSONDecodeError
from pathlib import Path

import pyarrow.dataset as ds

from iceberg_bioimage.models.scan_result import (
    ContractValidationResult,
    ScanResult,
    WarehouseTableManifestEntry,
    WarehouseValidationResult,
)

MICROSCOPY_REQUIRED_JOIN_KEYS = ("dataset_id", "image_id")
MICROSCOPY_RECOMMENDED_JOIN_KEYS = ("plate_id", "well_id", "site_id")
MICROSCOPY_PROFILE_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "dataset_id": (
        "Metadata_dataset_id",
        "Metadata_DatasetID",
        "Metadata_DatasetId",
        "Metadata_dataset",
        "Metadata_Source",
        "Metadata_source",
    ),
    "image_id": (
        "Metadata_image_id",
        "Metadata_ImageID",
        "Metadata_ImageId",
        "Metadata_image",
    ),
    "plate_id": (
        "Metadata_Plate",
        "Image_Metadata_Plate",
        "Metadata_plate",
    ),
    "well_id": (
        "Metadata_Well",
        "Image_Metadata_Well",
        "Metadata_well",
    ),
    "site_id": (
        "Metadata_Site",
        "Image_Metadata_Site",
        "Metadata_SiteNumber",
        "Metadata_Field",
        "Metadata_FOV",
        "Metadata_site",
    ),
}


def validate_scan_result(scan_result: ScanResult) -> list[str]:
    """Return validation errors for a scan result."""

    errors: list[str] = []
    if not scan_result.source_uri:
        errors.append("ScanResult.source_uri is required.")
    if not scan_result.image_assets:
        errors.append("ScanResult.image_assets must contain at least one asset.")

    for index, asset in enumerate(scan_result.image_assets):
        prefix = f"image_assets[{index}]"
        if not asset.uri:
            errors.append(f"{prefix}.uri is required.")
        if not asset.shape:
            errors.append(f"{prefix}.shape is required.")
        if not asset.dtype:
            errors.append(f"{prefix}.dtype is required.")

    return errors


def raise_for_invalid_scan_result(scan_result: ScanResult) -> None:
    """Raise a ValueError when a scan result is invalid."""

    errors = validate_scan_result(scan_result)
    if errors:
        raise ValueError("Invalid ScanResult: " + "; ".join(errors))


def validate_microscopy_profile_columns(
    columns: list[str] | tuple[str, ...],
    *,
    target: str = "profile_table",
    alias_map: Mapping[str, tuple[str, ...] | list[str]] | None = None,
) -> ContractValidationResult:
    """Validate a schema against the microscopy join contract."""

    present_columns = list(columns)
    resolved_columns = resolve_microscopy_profile_columns(
        present_columns,
        alias_map=alias_map,
    )
    missing_required = [
        column
        for column in MICROSCOPY_REQUIRED_JOIN_KEYS
        if resolved_columns[column] is None
    ]
    missing_recommended = [
        column
        for column in MICROSCOPY_RECOMMENDED_JOIN_KEYS
        if resolved_columns[column] is None
    ]

    warnings = _profile_contract_warnings(resolved_columns)
    if missing_recommended:
        warnings.append(
            "Recommended microscopy join keys are missing: "
            + ", ".join(missing_recommended)
        )

    return ContractValidationResult(
        target=target,
        present_columns=present_columns,
        required_columns=list(MICROSCOPY_REQUIRED_JOIN_KEYS),
        recommended_columns=list(MICROSCOPY_RECOMMENDED_JOIN_KEYS),
        missing_required_columns=missing_required,
        missing_recommended_columns=missing_recommended,
        warnings=warnings,
    )


def validate_microscopy_profile_table(path: str) -> ContractValidationResult:
    """Validate a local profile table file against the microscopy join contract."""

    try:
        dataset = ds.dataset(path)
    except Exception as exc:
        error = f"Invalid dataset path: {path}: {exc}"
        return ContractValidationResult(
            target=str(Path(path)),
            present_columns=[],
            required_columns=list(MICROSCOPY_REQUIRED_JOIN_KEYS),
            recommended_columns=list(MICROSCOPY_RECOMMENDED_JOIN_KEYS),
            missing_required_columns=list(MICROSCOPY_REQUIRED_JOIN_KEYS),
            missing_recommended_columns=list(MICROSCOPY_RECOMMENDED_JOIN_KEYS),
            warnings=[error],
        )
    return validate_microscopy_profile_columns(
        list(dataset.schema.names),
        target=str(Path(path)),
    )


def resolve_microscopy_profile_columns(
    columns: list[str] | tuple[str, ...],
    *,
    alias_map: Mapping[str, tuple[str, ...] | list[str]] | None = None,
) -> dict[str, str | None]:
    """Resolve canonical microscopy columns from a schema with known aliases."""

    present_columns = set(columns)
    resolved: dict[str, str | None] = {}
    aliases = {
        canonical: list(known_aliases)
        for canonical, known_aliases in MICROSCOPY_PROFILE_COLUMN_ALIASES.items()
    }
    if alias_map is not None:
        for canonical, custom_aliases in alias_map.items():
            aliases.setdefault(canonical, []).extend(custom_aliases)

    for column in (
        *MICROSCOPY_REQUIRED_JOIN_KEYS,
        *MICROSCOPY_RECOMMENDED_JOIN_KEYS,
    ):
        if column in present_columns:
            resolved[column] = column
            continue

        resolved[column] = next(
            (alias for alias in aliases.get(column, ()) if alias in present_columns),
            None,
        )

    return resolved


def profile_column_aliases() -> Mapping[str, tuple[str, ...]]:
    """Return the supported microscopy profile column aliases."""

    return MICROSCOPY_PROFILE_COLUMN_ALIASES


def load_profile_column_aliases(path: str | Path) -> dict[str, tuple[str, ...]]:
    """Load microscopy profile column aliases from a TOML file."""

    payload = tomllib.loads(Path(path).read_text())
    alias_section = payload.get("microscopy", {}).get("aliases", {})
    return {
        canonical: tuple(str(alias) for alias in aliases)
        for canonical, aliases in alias_section.items()
    }


def validate_warehouse_manifest(path: str | Path) -> WarehouseValidationResult:
    """Validate a manifest-backed warehouse root."""

    from iceberg_bioimage.integrations.cytomining import (
        _normalize_table_identifier,
        load_warehouse_manifest,
    )

    root = Path(path)
    result = WarehouseValidationResult(warehouse_root=str(root))
    manifest_path = root / "warehouse_manifest.json"
    if not manifest_path.exists():
        result.errors.append("warehouse_manifest.json is missing.")
        return result

    try:
        manifest = load_warehouse_manifest(root)
    except (JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        result.errors.append(f"Invalid warehouse_manifest.json: {exc}")
        return result

    if manifest.warehouse_spec_version is None:
        result.errors.append(
            "warehouse_manifest.json must declare warehouse_spec_version."
        )

    seen_table_names: set[str] = set()
    for table in manifest.tables:
        if table.table_name in seen_table_names:
            result.errors.append(
                f"Duplicate table_name in manifest: {table.table_name}"
            )
            continue
        seen_table_names.add(table.table_name)
        _validate_warehouse_manifest_table(
            root=root,
            table=table,
            result=result,
            normalize_table_identifier=_normalize_table_identifier,
        )

    return result


def _validate_warehouse_manifest_table(
    *,
    root: Path,
    table: WarehouseTableManifestEntry,
    result: WarehouseValidationResult,
    normalize_table_identifier: Callable[[str], tuple[str, tuple[str, ...]]],
) -> None:
    try:
        _, table_parts = normalize_table_identifier(table.table_name)
    except ValueError as exc:
        result.errors.append(f"Invalid manifest table_name {table.table_name!r}: {exc}")
        return

    if table.role == "quality_control" and table_parts[0] != "quality_control":
        result.errors.append(
            "Manifest table with role quality_control must use the "
            "quality_control namespace."
        )

    dataset_path = root.joinpath(*table_parts)
    if not dataset_path.exists():
        result.errors.append(f"Manifest table path does not exist: {table.table_name}")
        return

    if table.role in {"image_assets", "joined_profiles"} and not table.join_keys:
        result.errors.append(
            f"Manifest table {table.table_name} must declare join_keys."
        )

    if not table.columns:
        result.warnings.append(
            f"Manifest table {table.table_name} does not record columns."
        )


def _profile_contract_warnings(resolved_columns: dict[str, str | None]) -> list[str]:
    warnings: list[str] = []
    aliased_columns = [
        f"{canonical}<-{resolved}"
        for canonical, resolved in resolved_columns.items()
        if resolved is not None and canonical != resolved
    ]
    if aliased_columns:
        warnings.append(
            "Microscopy join keys will require alias normalization: "
            + ", ".join(aliased_columns)
        )

    return warnings
