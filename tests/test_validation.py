"""Validation helper tests."""

from __future__ import annotations

import json
from pathlib import Path

from iceberg_bioimage.validation.contracts import (
    load_profile_column_aliases,
    validate_microscopy_profile_columns,
    validate_microscopy_profile_table,
    validate_warehouse_manifest,
)


def test_validate_microscopy_profile_table_invalid_path() -> None:
    result = validate_microscopy_profile_table("/tmp/does-not-exist.parquet")

    assert result.is_valid is False
    assert result.target == str(Path("/tmp/does-not-exist.parquet"))
    assert any("Invalid dataset path" in warning for warning in result.warnings)


def test_validate_microscopy_profile_columns_accepts_common_aliases() -> None:
    result = validate_microscopy_profile_columns(
        [
            "Metadata_dataset_id",
            "Metadata_ImageID",
            "Metadata_Plate",
            "Metadata_Well",
            "Metadata_Site",
        ]
    )

    assert result.is_valid is True
    assert result.missing_required_columns == []
    assert result.missing_recommended_columns == []
    assert any("alias normalization" in warning for warning in result.warnings)


def test_validate_microscopy_profile_table_accepts_pycytominer_fixture() -> None:
    fixture_path = Path(__file__).parent / "data" / "profiles_pycytominer.parquet"

    result = validate_microscopy_profile_table(str(fixture_path))

    assert result.is_valid is True
    assert result.missing_required_columns == []
    assert result.missing_recommended_columns == []
    assert any("alias normalization" in warning for warning in result.warnings)


def test_load_profile_column_aliases_from_toml(tmp_path: Path) -> None:
    config_path = tmp_path / "aliases.toml"
    config_path.write_text(
        """
[microscopy.aliases]
dataset_id = ["ProjectID"]
image_id = ["ImageKey"]
"""
    )

    aliases = load_profile_column_aliases(config_path)
    result = validate_microscopy_profile_columns(
        ["ProjectID", "ImageKey"],
        alias_map=aliases,
    )

    assert aliases == {
        "dataset_id": ("ProjectID",),
        "image_id": ("ImageKey",),
    }
    assert result.is_valid is True


def test_validate_warehouse_manifest_missing_manifest(tmp_path: Path) -> None:
    result = validate_warehouse_manifest(tmp_path)

    assert result.is_valid is False
    assert result.errors == ["warehouse_manifest.json is missing."]


def test_validate_warehouse_manifest_rejects_malformed_table_name(
    tmp_path: Path,
) -> None:
    (tmp_path / "profiles" / "joined_profiles").mkdir(parents=True)
    (tmp_path / "warehouse_manifest.json").write_text(
        json.dumps(
            {
                "warehouse_root": str(tmp_path),
                "warehouse_spec_version": "1.0.0",
                "tables": [
                    {
                        "table_name": "profiles..joined_profiles",
                        "role": "joined_profiles",
                        "format": "parquet",
                        "join_keys": ["dataset_id", "image_id"],
                        "columns": ["dataset_id", "image_id"],
                    }
                ],
            }
        )
    )

    result = validate_warehouse_manifest(tmp_path)

    assert result.is_valid is False
    assert any("Invalid manifest table_name" in error for error in result.errors)


def test_validate_warehouse_manifest_requires_spec_version(tmp_path: Path) -> None:
    (tmp_path / "profiles" / "joined_profiles").mkdir(parents=True)
    (tmp_path / "warehouse_manifest.json").write_text(
        json.dumps(
            {
                "warehouse_root": str(tmp_path),
                "tables": [
                    {
                        "table_name": "profiles.joined_profiles",
                        "role": "joined_profiles",
                        "format": "parquet",
                        "join_keys": ["dataset_id", "image_id"],
                        "columns": ["dataset_id", "image_id"],
                    }
                ],
            }
        )
    )

    result = validate_warehouse_manifest(tmp_path)

    assert result.is_valid is False
    assert (
        "warehouse_manifest.json must declare warehouse_spec_version." in result.errors
    )


def test_validate_warehouse_manifest_requires_quality_control_namespace(
    tmp_path: Path,
) -> None:
    (tmp_path / "profiles" / "cosmicqc_profiles").mkdir(parents=True)
    (tmp_path / "warehouse_manifest.json").write_text(
        json.dumps(
            {
                "warehouse_root": str(tmp_path),
                "warehouse_spec_version": "1.0.0",
                "tables": [
                    {
                        "table_name": "profiles.cosmicqc_profiles",
                        "role": "quality_control",
                        "format": "parquet",
                        "join_keys": ["dataset_id", "image_id"],
                        "columns": ["dataset_id", "image_id", "qc_pass"],
                    }
                ],
            }
        )
    )

    result = validate_warehouse_manifest(tmp_path)

    assert result.is_valid is False
    assert (
        "Manifest table with role quality_control must use the quality_control "
        "namespace." in result.errors
    )
