"""Tests for Cytomining warehouse export helpers."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest
import zarr

from iceberg_bioimage import (
    export_catalog_to_cytomining_warehouse,
    export_profiles_to_cytomining_warehouse,
    export_store_to_cytomining_warehouse,
    export_table_to_cytomining_warehouse,
)
from iceberg_bioimage.integrations.cytomining import (
    DEFAULT_PROFILE_NAMESPACE,
    DEFAULT_WAREHOUSE_SPEC_VERSION,
    _catalog_table_leaf_name,
    export_scan_result_to_cytomining_warehouse,
    load_warehouse_manifest,
)
from iceberg_bioimage.models.scan_result import ImageAsset, ScanResult
from iceberg_bioimage.validation.contracts import validate_warehouse_manifest

EXPECTED_CHUNK_ROWS = 4


def test_export_scan_result_to_cytomining_warehouse(tmp_path: Path) -> None:
    scan_result = ScanResult(
        source_uri="/tmp/plate.zarr",
        format_family="zarr",
        image_assets=[
            ImageAsset(
                uri="/tmp/plate.zarr",
                array_path="0",
                shape=[4, 4],
                dtype="uint16",
                chunk_shape=[2, 2],
                image_id="plate:0",
            )
        ],
    )
    warehouse_root = tmp_path / "warehouse"

    result = export_scan_result_to_cytomining_warehouse(
        scan_result,
        warehouse_root,
        include_chunks=True,
    )

    assert result.warehouse_root == str(warehouse_root)
    assert result.tables_written == ["images.image_assets", "images.chunk_index"]
    assert result.row_counts == {
        "images.image_assets": 1,
        "images.chunk_index": 4,
    }
    assert result.manifest_path == str(warehouse_root / "warehouse_manifest.json")
    assert (
        ds.dataset(warehouse_root / "images" / "image_assets").to_table().num_rows == 1
    )
    assert (
        ds.dataset(warehouse_root / "images" / "chunk_index").to_table().num_rows
        == EXPECTED_CHUNK_ROWS
    )


def test_export_scan_result_to_cytomining_warehouse_normalizes_bare_table_names(
    tmp_path: Path,
) -> None:
    scan_result = ScanResult(
        source_uri="/tmp/plate.zarr",
        format_family="zarr",
        image_assets=[
            ImageAsset(
                uri="/tmp/plate.zarr",
                array_path="0",
                shape=[4, 4],
                dtype="uint16",
                chunk_shape=[2, 2],
                image_id="plate:0",
            )
        ],
    )
    warehouse_root = tmp_path / "warehouse"

    result = export_scan_result_to_cytomining_warehouse(
        scan_result,
        warehouse_root,
        include_chunks=False,
        image_assets_table_name="image_assets",
        joined_table_name="joined_profiles",
    )

    assert result.tables_written == ["images.image_assets"]
    assert result.row_counts == {"images.image_assets": 1}
    assert (
        ds.dataset(warehouse_root / "images" / "image_assets").to_table().num_rows == 1
    )


def test_export_store_to_cytomining_warehouse_with_pycytominer_fixture(
    tmp_path: Path,
) -> None:
    pytest.importorskip("duckdb")

    store_path = tmp_path / "plate.zarr"
    root = zarr.open_group(store_path, mode="w", zarr_version=2)
    data = np.arange(12, dtype=np.uint16).reshape(3, 4)
    root.create_dataset("0", shape=data.shape, data=data, chunks=(2, 2))
    warehouse_root = tmp_path / "warehouse"
    profile_table = Path(__file__).parent / "data" / "profiles_pycytominer.parquet"

    result = export_store_to_cytomining_warehouse(
        str(store_path),
        warehouse_root,
        profiles=profile_table,
        include_chunks=False,
    )

    assert result.row_counts == {
        "images.image_assets": 1,
        "profiles.joined_profiles": 1,
    }
    joined_profiles = ds.dataset(
        warehouse_root / "profiles" / "joined_profiles"
    ).to_table()
    assert joined_profiles.to_pydict()["image_id"] == ["plate:0"]
    assert joined_profiles.to_pydict()["AreaShape_Area"] == [101.5]


def test_export_store_to_cytomining_warehouse_appends_to_existing_root(
    tmp_path: Path,
) -> None:
    pytest.importorskip("duckdb")

    first_store = tmp_path / "plate.zarr"
    first_root = zarr.open_group(first_store, mode="w", zarr_version=2)
    first_root.create_dataset(
        "0",
        shape=(3, 4),
        data=np.arange(12, dtype=np.uint16).reshape(3, 4),
        chunks=(2, 2),
    )

    second_store = tmp_path / "cells.ome.zarr"
    second_root = zarr.open_group(second_store, mode="w", zarr_version=2)
    second_root.create_dataset(
        "0",
        shape=(2, 3),
        data=np.arange(6, dtype=np.uint16).reshape(2, 3),
        chunks=(1, 3),
    )

    warehouse_root = tmp_path / "warehouse"
    first_profiles = tmp_path / "profiles_first.parquet"
    second_profiles = tmp_path / "profiles_second.parquet"
    pq.write_table(
        pa.table(
            {
                "dataset_id": ["plate"],
                "image_id": ["plate:0"],
                "cell_count": [10],
            }
        ),
        first_profiles,
    )
    pq.write_table(
        pa.table(
            {
                "dataset_id": ["cells"],
                "image_id": ["cells:0"],
                "cell_count": [20],
            }
        ),
        second_profiles,
    )

    export_store_to_cytomining_warehouse(
        str(first_store),
        warehouse_root,
        profiles=first_profiles,
        include_chunks=False,
        mode="overwrite",
    )
    export_store_to_cytomining_warehouse(
        str(second_store),
        warehouse_root,
        profiles=second_profiles,
        include_chunks=False,
        mode="append",
    )

    image_assets = ds.dataset(warehouse_root / "images" / "image_assets").to_table()
    joined_profiles = ds.dataset(
        warehouse_root / "profiles" / "joined_profiles"
    ).to_table()
    assert sorted(image_assets.to_pydict()["dataset_id"]) == ["cells", "plate"]
    assert sorted(joined_profiles.to_pydict()["cell_count"]) == [10, 20]


def test_export_profiles_to_cytomining_warehouse_appends_named_tables(
    tmp_path: Path,
) -> None:
    warehouse_root = tmp_path / "warehouse"
    pycytominer_profiles = (
        Path(__file__).parent / "data" / "profiles_pycytominer.parquet"
    )
    cosmicqc_profiles = Path(__file__).parent / "data" / "profiles_cosmicqc.parquet"

    pycytominer_result = export_profiles_to_cytomining_warehouse(
        pycytominer_profiles,
        warehouse_root,
        table_name="pycytominer_profiles",
        mode="append",
    )
    cosmicqc_result = export_profiles_to_cytomining_warehouse(
        cosmicqc_profiles,
        warehouse_root,
        table_name="cosmicqc_profiles",
        role="quality_control",
        profile_dataset_id="plate",
        mode="append",
    )

    assert pycytominer_result.row_counts == {"profiles.pycytominer_profiles": 1}
    assert cosmicqc_result.row_counts == {"quality_control.cosmicqc_profiles": 1}

    pycytominer_table = ds.dataset(
        warehouse_root / "profiles" / "pycytominer_profiles"
    ).to_table()
    cosmicqc_table = ds.dataset(
        warehouse_root / "quality_control" / "cosmicqc_profiles"
    ).to_table()

    assert pycytominer_table.to_pydict()["dataset_id"] == ["plate"]
    assert pycytominer_table.to_pydict()["image_id"] == ["plate:0"]
    assert cosmicqc_table.to_pydict()["dataset_id"] == ["plate"]
    assert cosmicqc_table.to_pydict()["image_id"] == ["plate:0"]
    assert cosmicqc_table.to_pydict()["QC_Pass"] == [True]

    manifest = load_warehouse_manifest(warehouse_root)
    assert manifest.warehouse_spec_version == DEFAULT_WAREHOUSE_SPEC_VERSION
    manifest_tables = {table.table_name: table for table in manifest.tables}
    assert sorted(manifest_tables) == [
        "profiles.pycytominer_profiles",
        "quality_control.cosmicqc_profiles",
    ]
    assert manifest_tables["profiles.pycytominer_profiles"].role == "profiles"
    assert (
        manifest_tables["quality_control.cosmicqc_profiles"].role == "quality_control"
    )
    assert manifest_tables["quality_control.cosmicqc_profiles"].row_count == 1

    validation = validate_warehouse_manifest(warehouse_root)
    assert validation.is_valid is True


def test_export_table_to_cytomining_warehouse_supports_custom_role(
    tmp_path: Path,
) -> None:
    warehouse_root = tmp_path / "warehouse"
    result = export_table_to_cytomining_warehouse(
        pa.table(
            {
                "dataset_id": ["plate"],
                "image_id": ["plate:0"],
                "embedding_0": [0.1],
                "embedding_1": [0.2],
            }
        ),
        warehouse_root,
        table_name="embeddings",
        role="embeddings",
        join_keys=["dataset_id", "image_id"],
        source_type="custom",
        source_ref="unit-test",
        mode="append",
        default_namespace=DEFAULT_PROFILE_NAMESPACE,
    )

    assert result.row_counts == {"profiles.embeddings": 1}
    manifest = load_warehouse_manifest(warehouse_root)
    assert manifest.warehouse_spec_version == DEFAULT_WAREHOUSE_SPEC_VERSION
    embeddings_entry = next(
        table for table in manifest.tables if table.table_name == "profiles.embeddings"
    )
    assert embeddings_entry.role == "embeddings"
    assert embeddings_entry.join_keys == ["dataset_id", "image_id"]
    assert embeddings_entry.source_ref == "unit-test"


def test_export_table_to_cytomining_warehouse_requires_quality_control_namespace(
    tmp_path: Path,
) -> None:
    warehouse_root = tmp_path / "warehouse"
    with pytest.raises(
        ValueError,
        match="quality_control role must use the quality_control namespace",
    ):
        export_table_to_cytomining_warehouse(
            pa.table(
                {
                    "dataset_id": ["plate"],
                    "image_id": ["plate:0"],
                    "qc_pass": [True],
                }
            ),
            warehouse_root,
            table_name="profiles.cosmicqc_profiles",
            role="quality_control",
            join_keys=["dataset_id", "image_id"],
            source_type="custom",
            source_ref="unit-test",
            mode="append",
        )


def test_export_table_to_cytomining_warehouse_normalizes_table_identifier(
    tmp_path: Path,
) -> None:
    warehouse_root = tmp_path / "warehouse"
    result = export_table_to_cytomining_warehouse(
        pa.table(
            {
                "dataset_id": ["plate"],
                "image_id": ["plate:0"],
            }
        ),
        warehouse_root,
        table_name=" profiles . joined_profiles ",
        role="joined_profiles",
        join_keys=["dataset_id", "image_id"],
    )

    assert result.tables_written == ["profiles.joined_profiles"]
    assert result.row_counts == {"profiles.joined_profiles": 1}
    assert (
        ds.dataset(warehouse_root / "profiles" / "joined_profiles").to_table().num_rows
        == 1
    )


def test_export_table_to_cytomining_warehouse_rejects_empty_table_name_segment(
    tmp_path: Path,
) -> None:
    warehouse_root = tmp_path / "warehouse"

    with pytest.raises(ValueError, match="malformed table_name: empty segment"):
        export_table_to_cytomining_warehouse(
            pa.table(
                {
                    "dataset_id": ["plate"],
                    "image_id": ["plate:0"],
                }
            ),
            warehouse_root,
            table_name="profiles..joined_profiles",
            role="joined_profiles",
            join_keys=["dataset_id", "image_id"],
        )


def test_export_catalog_to_cytomining_warehouse_reads_catalog_leaf_names(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    read_tables: list[str] = []

    def fake_catalog_table_to_arrow(
        catalog: object,
        namespace: object,
        table_name: str,
        *,
        scan_options: object | None = None,
    ) -> pa.Table:
        del catalog, namespace, scan_options
        read_tables.append(table_name)
        if table_name == "image_assets":
            return pa.table({"dataset_id": ["plate"], "image_id": ["plate:0"]})
        if table_name == "chunk_index":
            return pa.table(
                {
                    "dataset_id": ["plate"],
                    "image_id": ["plate:0"],
                    "array_path": ["0"],
                }
            )
        raise AssertionError(f"unexpected table_name {table_name!r}")

    monkeypatch.setattr(
        "iceberg_bioimage.integrations.catalog.catalog_table_to_arrow",
        fake_catalog_table_to_arrow,
    )

    result = export_catalog_to_cytomining_warehouse(
        catalog="default",
        namespace="bioimage.cytotable",
        warehouse_root=tmp_path / "warehouse",
        profiles=None,
    )

    assert read_tables == ["image_assets", "chunk_index"]
    assert result.tables_written == ["images.image_assets", "images.chunk_index"]


def test_export_catalog_to_cytomining_warehouse_supports_explicit_catalog_names(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    read_tables: list[str] = []

    def fake_catalog_table_to_arrow(
        catalog: object,
        namespace: object,
        table_name: str,
        *,
        scan_options: object | None = None,
    ) -> pa.Table:
        del catalog, namespace, scan_options
        read_tables.append(table_name)
        return pa.table({"dataset_id": ["plate"], "image_id": ["plate:0"]})

    monkeypatch.setattr(
        "iceberg_bioimage.integrations.catalog.catalog_table_to_arrow",
        fake_catalog_table_to_arrow,
    )

    result = export_catalog_to_cytomining_warehouse(
        catalog="default",
        namespace="bioimage.cytotable",
        warehouse_root=tmp_path / "warehouse",
        profiles=None,
        image_assets_table_name="images.custom_image_assets",
        chunk_index_table_name=None,
        catalog_image_assets_table_name="image_assets_v2",
    )

    assert read_tables == ["image_assets_v2"]
    assert result.tables_written == ["images.custom_image_assets"]


def test_catalog_table_leaf_name_strips_and_validates() -> None:
    assert _catalog_table_leaf_name(" images.image_assets ") == "image_assets"
    assert _catalog_table_leaf_name("image_assets") == "image_assets"

    with pytest.raises(ValueError, match="empty leaf segment"):
        _catalog_table_leaf_name("images.")
    with pytest.raises(ValueError, match="empty leaf segment"):
        _catalog_table_leaf_name(".image_assets")

    with pytest.raises(ValueError, match="illegal leaf segment"):
        _catalog_table_leaf_name("images.image assets")


def test_export_table_to_cytomining_warehouse_normalizes_stale_spec_version(
    tmp_path: Path,
) -> None:
    warehouse_root = tmp_path / "warehouse"
    warehouse_root.mkdir(parents=True, exist_ok=True)
    (warehouse_root / "warehouse_manifest.json").write_text(
        """
{
  "warehouse_root": "stale-root",
  "warehouse_spec_version": "0.9.0",
  "tables": []
}
""".strip()
    )

    export_table_to_cytomining_warehouse(
        pa.table({"dataset_id": ["plate"], "image_id": ["plate:0"]}),
        warehouse_root,
        table_name="profiles.joined_profiles",
        role="joined_profiles",
        join_keys=["dataset_id", "image_id"],
    )

    manifest = load_warehouse_manifest(warehouse_root)
    assert manifest.warehouse_spec_version == DEFAULT_WAREHOUSE_SPEC_VERSION


def test_export_table_to_cytomining_warehouse_normalizes_legacy_manifest_table_names(
    tmp_path: Path,
) -> None:
    warehouse_root = tmp_path / "warehouse"
    warehouse_root.mkdir(parents=True, exist_ok=True)
    (warehouse_root / "warehouse_manifest.json").write_text(
        """
{
  "warehouse_root": "stale-root",
  "warehouse_spec_version": "0.9.0",
  "tables": [
    {
      "table_name": "image_assets",
      "role": "image_assets",
      "format": "parquet",
      "join_keys": ["dataset_id", "image_id"],
      "columns": ["dataset_id", "image_id"]
    }
  ]
}
""".strip()
    )

    export_table_to_cytomining_warehouse(
        pa.table({"dataset_id": ["plate"], "image_id": ["plate:0"]}),
        warehouse_root,
        table_name="profiles.joined_profiles",
        role="joined_profiles",
        join_keys=["dataset_id", "image_id"],
    )

    manifest = load_warehouse_manifest(warehouse_root)
    assert manifest.warehouse_spec_version == DEFAULT_WAREHOUSE_SPEC_VERSION
    assert sorted(table.table_name for table in manifest.tables) == [
        "images.image_assets",
        "profiles.joined_profiles",
    ]


def test_export_table_to_cytomining_warehouse_falls_back_for_unknown_legacy_table_names(
    tmp_path: Path,
) -> None:
    warehouse_root = tmp_path / "warehouse"
    warehouse_root.mkdir(parents=True, exist_ok=True)
    (warehouse_root / "warehouse_manifest.json").write_text(
        """
{
  "warehouse_root": "stale-root",
  "warehouse_spec_version": "0.9.0",
  "tables": [
    {
      "table_name": "custom table",
      "role": "custom_role",
      "format": "parquet",
      "join_keys": ["dataset_id", "image_id"],
      "columns": ["dataset_id", "image_id"]
    }
  ]
}
""".strip()
    )

    export_table_to_cytomining_warehouse(
        pa.table({"dataset_id": ["plate"], "image_id": ["plate:0"]}),
        warehouse_root,
        table_name="profiles.joined_profiles",
        role="joined_profiles",
        join_keys=["dataset_id", "image_id"],
    )

    manifest = load_warehouse_manifest(warehouse_root)
    assert manifest.warehouse_spec_version == DEFAULT_WAREHOUSE_SPEC_VERSION
    assert sorted(table.table_name for table in manifest.tables) == [
        "legacy.custom_table",
        "profiles.joined_profiles",
    ]
