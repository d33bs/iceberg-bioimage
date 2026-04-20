"""CLI tests."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import zarr
from pytest import CaptureFixture, MonkeyPatch

from iceberg_bioimage import cli as cli_module

CLI_VALUE_ERROR_EXIT_CODE = 2


def test_scan_cli(tmp_path: Path) -> None:
    store_path = tmp_path / "demo.zarr"
    root = zarr.open_group(store_path, mode="w")
    data = np.arange(6, dtype=np.uint8).reshape(2, 3)
    root.create_array("0", data=data, chunks=(1, 3))

    output = subprocess.run(
        [sys.executable, "-m", "iceberg_bioimage.cli", "scan", str(store_path)],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )

    assert "format_family: zarr" in output.stdout
    assert "shape=[2, 3]" in output.stdout


def test_validate_contract_cli(tmp_path: Path) -> None:
    table_path = tmp_path / "cells.parquet"
    pq.write_table(
        pa.table(
            {
                "dataset_id": ["ds-1"],
                "image_id": ["img-1"],
                "value": [1],
            }
        ),
        table_path,
    )

    output = subprocess.run(
        [
            sys.executable,
            "-m",
            "iceberg_bioimage.cli",
            "validate-contract",
            str(table_path),
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )

    assert "is_valid: True" in output.stdout
    assert "missing_recommended_columns:" in output.stdout


def test_summarize_cli(monkeypatch: MonkeyPatch, capsys: CaptureFixture[str]) -> None:
    def _fake_summarize_store(uri: str) -> object:
        assert uri == "data/example.ome.zarr"
        return SimpleNamespace(
            source_uri=uri,
            format_family="zarr",
            image_asset_count=2,
            chunked_asset_count=1,
            array_paths=["0", "1"],
            dtypes=["uint16"],
            shapes=[[1, 256, 256], [1, 128, 128]],
            axes=["cyx"],
            channel_counts=[1],
            storage_variants=["zarr-v2"],
            warnings=[],
        )

    monkeypatch.setattr(cli_module, "summarize_store", _fake_summarize_store)

    exit_code = cli_module.main(["summarize", "data/example.ome.zarr"])
    output = capsys.readouterr()

    assert exit_code == 0
    assert "image_asset_count: 2" in output.out
    assert "storage_variants: zarr-v2" in output.out


def test_register_cli_publish_chunks(
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    def _fake_register_store(
        uri: str,
        catalog: str,
        namespace: str,
        *,
        image_assets_table: str = "image_assets",
        chunk_index_table: str | None = "chunk_index",
    ) -> object:
        assert uri == "data/example.zarr"
        assert catalog == "default"
        assert namespace == "bioimage"
        assert image_assets_table == "image_assets"
        assert chunk_index_table == "chunk_index"

        return SimpleNamespace(
            image_assets_rows_published=1,
            chunk_rows_published=4,
            source_uri=uri,
        )

    monkeypatch.setattr(
        cli_module,
        "register_store",
        _fake_register_store,
    )

    exit_code = cli_module.main(
        [
            "register",
            "--catalog",
            "default",
            "--namespace",
            "bioimage",
            "--publish-chunks",
            "data/example.zarr",
        ]
    )
    output = capsys.readouterr()

    assert exit_code == 0
    assert '"chunk_rows_published": 4' in output.out


def test_publish_chunks_cli(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    store_path = tmp_path / "demo.zarr"
    root = zarr.open_group(store_path, mode="w")
    data = np.arange(6, dtype=np.uint8).reshape(2, 3)
    root.create_array("0", data=data, chunks=(1, 3))

    def _fake_publish_chunk_index(
        catalog: str,
        namespace: str,
        table_name: str,
        scan_result: object,
    ) -> int:
        assert catalog == "default"
        assert namespace == "bioimage"
        assert table_name == "chunk_index"
        assert getattr(scan_result, "source_uri", None) == str(store_path)
        return 2

    monkeypatch.setattr(
        cli_module,
        "publish_chunk_index",
        _fake_publish_chunk_index,
    )

    exit_code = cli_module.main(
        [
            "publish-chunks",
            "--catalog",
            "default",
            "--namespace",
            "bioimage",
            str(store_path),
        ]
    )
    output = capsys.readouterr()

    assert exit_code == 0
    assert '"rows_published": 2' in output.out


def test_ingest_cli(
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    def _fake_ingest_stores_to_warehouse(
        uris: list[str],
        catalog: str,
        namespace: str,
        *,
        image_assets_table: str = "image_assets",
        chunk_index_table: str | None = "chunk_index",
    ) -> object:
        assert uris == ["data/a.zarr", "data/b.zarr"]
        assert catalog == "default"
        assert namespace == "bioimage"
        assert image_assets_table == "image_assets"
        assert chunk_index_table == "chunk_index"
        return SimpleNamespace(
            to_json=lambda **kwargs: cli_module.json.dumps(
                {
                    "catalog": catalog,
                    "namespace": ["bioimage"],
                    "dataset_count": 2,
                    "image_assets_rows_published": 2,
                    "chunk_rows_published": 4,
                },
                **kwargs,
            )
        )

    monkeypatch.setattr(
        cli_module,
        "ingest_stores_to_warehouse",
        _fake_ingest_stores_to_warehouse,
    )

    exit_code = cli_module.main(
        [
            "ingest",
            "--catalog",
            "default",
            "--namespace",
            "bioimage",
            "data/a.zarr",
            "data/b.zarr",
        ]
    )
    output = capsys.readouterr()

    assert exit_code == 0
    assert '"dataset_count": 2' in output.out


def test_export_cytomining_cli(
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    def _fake_export_store_to_cytomining_warehouse(  # noqa: PLR0913
        uri: str,
        warehouse_root: str,
        *,
        profiles: str | None = None,
        include_chunks: bool = True,
        image_assets_table_name: str = "images.image_assets",
        chunk_index_table_name: str = "images.chunk_index",
        joined_table_name: str = "profiles.joined_profiles",
        profile_dataset_id: str | None = None,
        mode: str = "overwrite",
    ) -> object:
        assert uri == "data/example.zarr"
        assert warehouse_root == "warehouse-root"
        assert profiles == "data/profiles.parquet"
        assert include_chunks is True
        assert image_assets_table_name == "images.image_assets"
        assert chunk_index_table_name == "images.chunk_index"
        assert joined_table_name == "profiles.joined_profiles"
        assert profile_dataset_id == "plate"
        assert mode == "append"
        return SimpleNamespace(
            to_json=lambda **kwargs: cli_module.json.dumps(
                {
                    "warehouse_root": warehouse_root,
                    "tables_written": [
                        "images.image_assets",
                        "profiles.joined_profiles",
                    ],
                    "row_counts": {
                        "images.image_assets": 1,
                        "profiles.joined_profiles": 1,
                    },
                },
                **kwargs,
            )
        )

    monkeypatch.setattr(
        cli_module,
        "export_store_to_cytomining_warehouse",
        _fake_export_store_to_cytomining_warehouse,
    )

    exit_code = cli_module.main(
        [
            "export-cytomining",
            "--warehouse-root",
            "warehouse-root",
            "--profiles",
            "data/profiles.parquet",
            "--profile-dataset-id",
            "plate",
            "--mode",
            "append",
            "data/example.zarr",
        ]
    )
    output = capsys.readouterr()

    assert exit_code == 0
    assert '"tables_written": [' in output.out


def test_export_cytomining_catalog_cli(
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    def _fake_export_catalog_to_cytomining_warehouse(  # noqa: PLR0913
        catalog: str,
        namespace: str,
        warehouse_root: str,
        *,
        profiles: str | None = None,
        image_assets_table_name: str = "images.image_assets",
        chunk_index_table_name: str | None = "images.chunk_index",
        joined_table_name: str = "profiles.joined_profiles",
        profile_dataset_id: str | None = None,
        mode: str = "overwrite",
    ) -> object:
        assert catalog == "default"
        assert namespace == "bioimage"
        assert warehouse_root == "warehouse-root"
        assert profiles == "data/profiles.parquet"
        assert image_assets_table_name == "custom_image_assets"
        assert chunk_index_table_name is None
        assert joined_table_name == "profiles.joined_profiles"
        assert profile_dataset_id == "plate"
        assert mode == "append"
        return SimpleNamespace(
            to_json=lambda **kwargs: cli_module.json.dumps(
                {
                    "warehouse_root": warehouse_root,
                    "tables_written": [
                        "custom_image_assets",
                        "profiles.joined_profiles",
                    ],
                    "row_counts": {
                        "custom_image_assets": 1,
                        "profiles.joined_profiles": 1,
                    },
                },
                **kwargs,
            )
        )

    monkeypatch.setattr(
        cli_module,
        "export_catalog_to_cytomining_warehouse",
        _fake_export_catalog_to_cytomining_warehouse,
    )

    exit_code = cli_module.main(
        [
            "export-cytomining-catalog",
            "--catalog",
            "default",
            "--namespace",
            "bioimage",
            "--warehouse-root",
            "warehouse-root",
            "--profiles",
            "data/profiles.parquet",
            "--image-assets-table",
            "custom_image_assets",
            "--skip-chunks",
            "--profile-dataset-id",
            "plate",
            "--mode",
            "append",
        ]
    )
    output = capsys.readouterr()

    assert exit_code == 0
    assert '"custom_image_assets"' in output.out


def test_export_cytomining_profiles_cli(
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    def _fake_export_profiles_to_cytomining_warehouse(  # noqa: PLR0913
        profiles: str,
        warehouse_root: str,
        *,
        table_name: str = "profiles",
        role: str = "profiles",
        profile_dataset_id: str | None = None,
        mode: str = "append",
    ) -> object:
        assert profiles == "data/profiles.parquet"
        assert warehouse_root == "warehouse-root"
        assert table_name == "cosmicqc_profiles"
        assert role == "quality_control"
        assert profile_dataset_id == "plate"
        assert mode == "append"
        return SimpleNamespace(
            to_json=lambda **kwargs: cli_module.json.dumps(
                {
                    "warehouse_root": warehouse_root,
                    "tables_written": [table_name],
                    "row_counts": {table_name: 1},
                },
                **kwargs,
            )
        )

    monkeypatch.setattr(
        cli_module,
        "export_profiles_to_cytomining_warehouse",
        _fake_export_profiles_to_cytomining_warehouse,
    )

    exit_code = cli_module.main(
        [
            "export-cytomining-profiles",
            "--warehouse-root",
            "warehouse-root",
            "--table-name",
            "cosmicqc_profiles",
            "--role",
            "quality_control",
            "--profile-dataset-id",
            "plate",
            "data/profiles.parquet",
        ]
    )
    output = capsys.readouterr()

    assert exit_code == 0
    assert '"cosmicqc_profiles"' in output.out


def test_join_profiles_cli(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    output_path = tmp_path / "joined.parquet"

    def _fake_join_profiles_with_store(
        uri: str,
        profile_table: str,
        *,
        include_chunks: bool = False,
        profile_dataset_id: str | None = None,
    ) -> pa.Table:
        assert uri == "data/example.zarr"
        assert profile_table == "data/profiles.parquet"
        assert include_chunks is True
        assert profile_dataset_id is None
        return pa.table(
            {
                "dataset_id": ["example"],
                "image_id": ["example:0"],
                "cell_count": [5],
            }
        )

    monkeypatch.setattr(
        cli_module,
        "join_profiles_with_store",
        _fake_join_profiles_with_store,
    )

    exit_code = cli_module.main(
        [
            "join-profiles",
            "--output",
            str(output_path),
            "--include-chunks",
            "data/example.zarr",
            "data/profiles.parquet",
        ]
    )
    output = capsys.readouterr()
    written = pq.read_table(output_path)

    assert exit_code == 0
    assert written.to_pydict()["cell_count"] == [5]
    assert '"rows_written": 1' in output.out


def test_main_returns_cli_error_for_value_error(
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    def raise_value_error(args: object) -> int:
        raise ValueError("bad dataset")

    monkeypatch.setattr(
        cli_module,
        "_handle_scan",
        raise_value_error,
    )

    exit_code = cli_module.main(["scan", "data/missing.zarr"])
    output = capsys.readouterr()

    assert exit_code == CLI_VALUE_ERROR_EXIT_CODE
    assert "Error: bad dataset" in output.err


def test_main_returns_cli_error_for_runtime_error(
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    def raise_runtime_error(
        uri: str,
        profile_table: str,
        *,
        include_chunks: bool = False,
        profile_dataset_id: str | None = None,
    ) -> pa.Table:
        assert uri == "data/example.zarr"
        assert profile_table == "data/profiles.parquet"
        assert include_chunks is False
        assert profile_dataset_id is None
        raise RuntimeError("optional duckdb dependency group")

    monkeypatch.setattr(
        cli_module,
        "join_profiles_with_store",
        raise_runtime_error,
    )

    exit_code = cli_module.main(
        [
            "join-profiles",
            "--output",
            "joined.parquet",
            "data/example.zarr",
            "data/profiles.parquet",
        ]
    )
    output = capsys.readouterr()

    assert exit_code == CLI_VALUE_ERROR_EXIT_CODE
    assert "Error: optional duckdb dependency group" in output.err
