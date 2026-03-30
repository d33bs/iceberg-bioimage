"""Tests for public scanning, summary, and join APIs."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import tifffile
import zarr

from iceberg_bioimage import (
    ImageAsset,
    ScanResult,
    join_profiles_with_scan_result,
    join_profiles_with_store,
    scan_store,
    summarize_scan_result,
    summarize_store,
)

EXPECTED_CHANNEL_COUNT = 2
EXPECTED_CHUNK_ROW_COUNT = 4


def test_scan_store_reads_zarr(tmp_path: Path) -> None:
    store_path = tmp_path / "plate.zarr"
    root = zarr.open_group(store_path, mode="w", zarr_version=2)
    data = np.arange(12, dtype=np.uint16).reshape(3, 4)
    root.create_array("0", data=data, chunks=(2, 2))

    scan = scan_store(str(store_path))

    assert scan.format_family == "zarr"
    assert scan.source_uri == str(store_path)
    assert len(scan.image_assets) == 1
    asset = scan.image_assets[0]
    assert asset.array_path == "0"
    assert asset.shape == [3, 4]
    assert asset.chunk_shape == [2, 2]
    assert asset.dtype == "uint16"


def test_scan_store_reads_tiff(tmp_path: Path) -> None:
    image_path = tmp_path / "cells.ome.tiff"
    tifffile.imwrite(image_path, np.zeros((2, 3, 4), dtype=np.uint8))

    scan = scan_store(str(image_path))

    assert scan.format_family == "ome-tiff"
    assert len(scan.image_assets) == 1
    asset = scan.image_assets[0]
    assert asset.shape == [2, 3, 4]
    assert asset.dtype == "uint8"
    assert asset.metadata["channel_count"] is None
    assert asset.metadata["axes"] == "YXS"


def test_scan_store_reads_local_zarr_v3_metadata(tmp_path: Path) -> None:
    store_path = tmp_path / "plate.ome.zarr"
    store_path.mkdir()
    (store_path / "0").mkdir()
    (store_path / "zarr.json").write_text(
        json.dumps(
            {
                "zarr_format": 3,
                "node_type": "group",
                "attributes": {
                    "multiscales": [
                        {
                            "axes": ["c", "y", "x"],
                            "datasets": [{"path": "0"}],
                        }
                    ]
                },
            }
        )
    )
    (store_path / "0" / "zarr.json").write_text(
        json.dumps(
            {
                "zarr_format": 3,
                "node_type": "array",
                "shape": [2, 32, 16],
                "data_type": "uint16",
                "chunk_grid": {
                    "name": "regular",
                    "configuration": {"chunk_shape": [1, 16, 16]},
                },
            }
        )
    )

    scan = scan_store(str(store_path))

    assert scan.format_family == "zarr"
    assert len(scan.image_assets) == 1
    asset = scan.image_assets[0]
    assert asset.image_id == "plate:0"
    assert asset.shape == [2, 32, 16]
    assert asset.chunk_shape == [1, 16, 16]
    assert asset.dtype == "uint16"
    assert asset.metadata["storage_variant"] == "zarr-v3"
    assert asset.metadata["axes"] == "cyx"
    assert asset.metadata["channel_count"] == EXPECTED_CHANNEL_COUNT


def test_summarize_store_reports_storage_variants(tmp_path: Path) -> None:
    store_path = tmp_path / "plate.zarr"
    root = zarr.open_group(store_path, mode="w", zarr_version=2)
    root.attrs["multiscales"] = [
        {
            "axes": ["c", "y", "x"],
            "datasets": [{"path": "0"}],
        }
    ]
    data = np.arange(12, dtype=np.uint16).reshape(1, 3, 4)
    root.create_array("0", data=data, chunks=(1, 3, 2))

    summary = summarize_store(str(store_path))

    assert summary.source_uri == str(store_path)
    assert summary.image_asset_count == 1
    assert summary.chunked_asset_count == 1
    assert summary.array_paths == ["0"]
    assert summary.dtypes == ["uint16"]
    assert summary.shapes == [[1, 3, 4]]
    assert summary.axes == ["cyx"]
    assert summary.channel_counts == [1]
    assert summary.storage_variants == ["zarr-v2"]


def test_scan_store_uses_nested_group_multiscales_metadata(tmp_path: Path) -> None:
    store_path = tmp_path / "plate.zarr"
    root = zarr.open_group(store_path, mode="w", zarr_version=2)
    series = root.create_group("series")
    series.attrs["multiscales"] = [
        {
            "axes": ["c", "y", "x"],
            "datasets": [{"path": "0"}],
        }
    ]
    data = np.arange(12, dtype=np.uint16).reshape(1, 3, 4)
    series.create_array("0", data=data, chunks=(1, 3, 2))

    scan = scan_store(str(store_path))

    assert scan.image_assets[0].array_path == "series/0"
    assert scan.image_assets[0].metadata["axes"] == "cyx"
    assert scan.image_assets[0].metadata["channel_count"] == 1


def test_scan_store_uses_metadata_owner_path_for_nested_arrays(tmp_path: Path) -> None:
    store_path = tmp_path / "plate.zarr"
    root = zarr.open_group(store_path, mode="w", zarr_version=2)
    series = root.create_group("series")
    series.attrs["multiscales"] = [
        {
            "axes": ["c", "y", "x"],
            "datasets": [{"path": "level/0"}],
        }
    ]
    level_group = series.create_group("level")
    data = np.arange(12, dtype=np.uint16).reshape(1, 3, 4)
    level_group.create_array("0", data=data, chunks=(1, 3, 2))

    scan = scan_store(str(store_path))

    assert scan.image_assets[0].array_path == "series/level/0"
    assert scan.image_assets[0].metadata["axes"] == "cyx"
    assert scan.image_assets[0].metadata["channel_count"] == 1


def test_scan_store_reads_local_zarr_v3_metadata_from_file_uri(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "plate.ome.zarr"
    store_path.mkdir()
    (store_path / "0").mkdir()
    (store_path / "zarr.json").write_text(
        json.dumps(
            {
                "zarr_format": 3,
                "node_type": "group",
                "attributes": {
                    "multiscales": [
                        {
                            "axes": ["c", "y", "x"],
                            "datasets": [{"path": "0"}],
                        }
                    ]
                },
            }
        )
    )
    (store_path / "0" / "zarr.json").write_text(
        json.dumps(
            {
                "zarr_format": 3,
                "node_type": "array",
                "shape": [2, 32, 16],
                "data_type": "uint16",
                "chunk_grid": {
                    "name": "regular",
                    "configuration": {"chunk_shape": [1, 16, 16]},
                },
            }
        )
    )

    scan = scan_store(store_path.resolve().as_uri())

    assert scan.format_family == "zarr"
    assert scan.image_assets[0].metadata["storage_variant"] == "zarr-v3"
    assert scan.image_assets[0].metadata["axes"] == "cyx"


def test_scan_store_skips_malformed_local_zarr_v3_metadata(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store_path = tmp_path / "plate.ome.zarr"
    store_path.mkdir()
    (store_path / "0").mkdir()
    (store_path / "bad").mkdir()
    (store_path / "zarr.json").write_text(
        json.dumps({"zarr_format": 3, "node_type": "group"})
    )
    (store_path / "0" / "zarr.json").write_text(
        json.dumps(
            {
                "zarr_format": 3,
                "node_type": "array",
                "shape": [2, 32, 16],
                "data_type": "uint16",
                "chunk_grid": {
                    "name": "regular",
                    "configuration": {"chunk_shape": [1, 16, 16]},
                },
            }
        )
    )
    (store_path / "bad" / "zarr.json").write_text("{not-json")

    with caplog.at_level("WARNING"):
        scan = scan_store(str(store_path))

    assert scan.image_assets[0].array_path == "0"
    assert "Skipping malformed zarr.json" in caplog.text


def test_scan_store_skips_non_object_local_zarr_v3_metadata(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store_path = tmp_path / "plate.ome.zarr"
    store_path.mkdir()
    (store_path / "0").mkdir()
    (store_path / "bad").mkdir()
    (store_path / "zarr.json").write_text(
        json.dumps({"zarr_format": 3, "node_type": "group"})
    )
    (store_path / "0" / "zarr.json").write_text(
        json.dumps(
            {
                "zarr_format": 3,
                "node_type": "array",
                "shape": [2, 32, 16],
                "data_type": "uint16",
                "chunk_grid": {
                    "name": "regular",
                    "configuration": {"chunk_shape": [1, 16, 16]},
                },
            }
        )
    )
    (store_path / "bad" / "zarr.json").write_text(json.dumps(["not", "an", "object"]))

    with caplog.at_level("WARNING"):
        scan = scan_store(str(store_path))

    assert scan.image_assets[0].array_path == "0"
    assert "Skipping non-object zarr.json" in caplog.text


def test_scan_store_skips_malformed_parent_v3_metadata(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store_path = tmp_path / "plate.ome.zarr"
    series_path = store_path / "series"
    level_path = series_path / "level"
    array_path = level_path / "0"
    array_path.mkdir(parents=True)
    (store_path / "zarr.json").write_text(
        json.dumps(
            {
                "zarr_format": 3,
                "node_type": "group",
                "attributes": {
                    "multiscales": [
                        {
                            "axes": ["c", "y", "x"],
                            "datasets": [{"path": "series/level/0"}],
                        }
                    ]
                },
            }
        )
    )
    (series_path / "zarr.json").write_text("{not-json")
    (level_path / "zarr.json").write_text(
        json.dumps({"zarr_format": 3, "node_type": "group"})
    )
    (array_path / "zarr.json").write_text(
        json.dumps(
            {
                "zarr_format": 3,
                "node_type": "array",
                "shape": [2, 32, 16],
                "data_type": "uint16",
                "chunk_grid": {
                    "name": "regular",
                    "configuration": {"chunk_shape": [1, 16, 16]},
                },
            }
        )
    )

    with caplog.at_level("WARNING"):
        scan = scan_store(str(store_path))

    assert scan.image_assets[0].array_path == "series/level/0"
    assert scan.image_assets[0].metadata["axes"] == "cyx"
    assert "Skipping malformed parent zarr.json" in caplog.text


def test_scan_store_skips_non_object_parent_v3_metadata(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store_path = tmp_path / "plate.ome.zarr"
    series_path = store_path / "series"
    level_path = series_path / "level"
    array_path = level_path / "0"
    array_path.mkdir(parents=True)
    (store_path / "zarr.json").write_text(
        json.dumps(
            {
                "zarr_format": 3,
                "node_type": "group",
                "attributes": {
                    "multiscales": [
                        {
                            "axes": ["c", "y", "x"],
                            "datasets": [{"path": "series/level/0"}],
                        }
                    ]
                },
            }
        )
    )
    (series_path / "zarr.json").write_text(json.dumps(["not", "an", "object"]))
    (level_path / "zarr.json").write_text(
        json.dumps({"zarr_format": 3, "node_type": "group"})
    )
    (array_path / "zarr.json").write_text(
        json.dumps(
            {
                "zarr_format": 3,
                "node_type": "array",
                "shape": [2, 32, 16],
                "data_type": "uint16",
                "chunk_grid": {
                    "name": "regular",
                    "configuration": {"chunk_shape": [1, 16, 16]},
                },
            }
        )
    )

    with caplog.at_level("WARNING"):
        scan = scan_store(str(store_path))

    assert scan.image_assets[0].array_path == "series/level/0"
    assert scan.image_assets[0].metadata["axes"] == "cyx"
    assert "Skipping non-object parent zarr.json" in caplog.text


def test_summarize_scan_result_includes_root_array_path() -> None:
    summary = summarize_scan_result(
        ScanResult(
            source_uri="memory://example",
            format_family="zarr",
            image_assets=[
                ImageAsset(
                    uri="memory://example",
                    array_path=None,
                    shape=[2, 2],
                    dtype="uint8",
                )
            ],
            warnings=["demo"],
        )
    )

    assert summary.array_paths == ["<root>"]
    assert summary.warnings == ["demo"]


def test_join_profiles_with_scan_result(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")

    store_path = tmp_path / "plate.zarr"
    root = zarr.open_group(store_path, mode="w", zarr_version=2)
    data = np.arange(12, dtype=np.uint16).reshape(3, 4)
    root.create_array("0", data=data, chunks=(2, 2))
    scan = scan_store(str(store_path))
    profiles = pa.table(
        {
            "dataset_id": ["plate"],
            "image_id": ["plate:0"],
            "cell_count": [9],
        }
    )

    joined = join_profiles_with_scan_result(scan, profiles, include_chunks=True)

    assert joined.num_rows == EXPECTED_CHUNK_ROW_COUNT
    assert joined.column("cell_count").to_pylist() == [9, 9, 9, 9]
    assert sorted(joined.column("chunk_key").to_pylist()) == [
        "0/0",
        "0/1",
        "1/0",
        "1/1",
    ]


def test_join_profiles_with_store_from_parquet(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")

    store_path = tmp_path / "plate.zarr"
    root = zarr.open_group(store_path, mode="w", zarr_version=2)
    data = np.arange(12, dtype=np.uint16).reshape(3, 4)
    root.create_array("0", data=data, chunks=(2, 2))

    profile_table = tmp_path / "profiles.parquet"
    pq.write_table(
        pa.table(
            {
                "dataset_id": ["plate"],
                "image_id": ["plate:0"],
                "cell_count": [11],
            }
        ),
        profile_table,
    )

    joined = join_profiles_with_store(str(store_path), profile_table)

    assert joined.to_pydict()["cell_count"] == [11]
    assert joined.to_pydict()["image_id"] == ["plate:0"]


def test_join_profiles_with_scan_result_rejects_invalid_profiles(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "plate.zarr"
    root = zarr.open_group(store_path, mode="w", zarr_version=2)
    data = np.arange(6, dtype=np.uint8).reshape(2, 3)
    root.create_array("0", data=data, chunks=(1, 3))
    scan = scan_store(str(store_path))
    profiles = pa.table({"dataset_id": ["plate"], "cell_count": [3]})

    try:
        join_profiles_with_scan_result(scan, profiles)
    except ValueError as exc:
        assert "image_id" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected invalid profiles to raise ValueError.")


def test_join_profiles_with_scan_result_reports_missing_duckdb(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store_path = tmp_path / "plate.zarr"
    root = zarr.open_group(store_path, mode="w", zarr_version=2)
    data = np.arange(6, dtype=np.uint8).reshape(2, 3)
    root.create_array("0", data=data, chunks=(1, 3))
    scan = scan_store(str(store_path))
    profiles = pa.table(
        {
            "dataset_id": ["plate"],
            "image_id": ["plate:0"],
            "cell_count": [3],
        }
    )

    def _raise_missing_duckdb(*args: object, **kwargs: object) -> pa.Table:
        raise RuntimeError("optional duckdb dependency group")

    monkeypatch.setattr(
        "iceberg_bioimage.api.join_image_assets_with_profiles",
        _raise_missing_duckdb,
    )

    with pytest.raises(RuntimeError, match="optional duckdb dependency group"):
        join_profiles_with_scan_result(scan, profiles)


def test_scan_store_rejects_unknown_format() -> None:
    try:
        scan_store("example.h5")
    except ValueError as exc:
        assert "Unsupported bioimage URI" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected scan_store to reject unsupported formats.")
