"""Tests for catalog-facing integration helpers."""

from __future__ import annotations

import pyarrow as pa
from pytest import MonkeyPatch

from iceberg_bioimage.integrations import catalog as catalog_module
from iceberg_bioimage.integrations.catalog import (
    CatalogScanOptions,
    catalog_table_to_arrow,
    join_catalog_image_assets_with_profiles,
    list_catalog_tables,
    load_catalog_table,
)


class FakeScan:
    """Simple scan stub."""

    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def to_arrow(self) -> pa.Table:
        return self.table


class FakeIcebergTable:
    """Simple Iceberg-like table stub."""

    def __init__(self, table: pa.Table) -> None:
        self.table = table
        self.calls: list[dict[str, object]] = []

    def scan(
        self,
        row_filter: str = "True",
        selected_fields: tuple[str, ...] = ("*",),
        case_sensitive: bool = True,
        snapshot_id: int | None = None,
        limit: int | None = None,
    ) -> FakeScan:
        self.calls.append(
            {
                "row_filter": row_filter,
                "selected_fields": selected_fields,
                "case_sensitive": case_sensitive,
                "snapshot_id": snapshot_id,
                "limit": limit,
            }
        )
        if selected_fields == ("*",):
            table = self.table
        else:
            table = self.table.select(list(selected_fields))
        if limit is not None:
            table = table.slice(0, limit)
        return FakeScan(table)


class FakeCatalog:
    """Simple catalog stub."""

    def __init__(self, tables: dict[tuple[str, ...], FakeIcebergTable]) -> None:
        self.tables = tables

    def load_table(self, identifier: tuple[str, ...]) -> FakeIcebergTable:
        return self.tables[identifier]

    def list_tables(self, namespace: tuple[str, ...]) -> list[tuple[str, ...]]:
        return [
            identifier for identifier in self.tables if identifier[:-1] == namespace
        ]


def test_load_catalog_table() -> None:
    fake_table = FakeIcebergTable(pa.table({"dataset_id": ["ds-1"]}))
    catalog = FakeCatalog({("bioimage", "image_assets"): fake_table})

    loaded = load_catalog_table(catalog, "bioimage", "image_assets")

    assert loaded is fake_table


def test_list_catalog_tables() -> None:
    catalog = FakeCatalog(
        {
            ("bioimage", "image_assets"): FakeIcebergTable(pa.table({})),
            ("bioimage", "chunk_index"): FakeIcebergTable(pa.table({})),
            ("other", "image_assets"): FakeIcebergTable(pa.table({})),
        }
    )

    assert list_catalog_tables(catalog, "bioimage") == [
        "chunk_index",
        "image_assets",
    ]


def test_catalog_table_to_arrow() -> None:
    fake_table = FakeIcebergTable(
        pa.table(
            {
                "dataset_id": ["ds-1"],
                "image_id": ["img-1"],
                "uri": ["data/example.zarr"],
            }
        )
    )
    catalog = FakeCatalog({("bioimage", "image_assets"): fake_table})

    result = catalog_table_to_arrow(
        catalog,
        "bioimage",
        "image_assets",
        scan_options=CatalogScanOptions(
            columns=["dataset_id", "image_id"],
            where="dataset_id = 'ds-1'",
            limit=1,
        ),
    )

    assert result.to_pydict() == {
        "dataset_id": ["ds-1"],
        "image_id": ["img-1"],
    }
    assert fake_table.calls == [
        {
            "row_filter": "dataset_id = 'ds-1'",
            "selected_fields": ("dataset_id", "image_id"),
            "case_sensitive": True,
            "snapshot_id": None,
            "limit": 1,
        }
    ]


def test_catalog_table_to_arrow_accepts_string_column_name() -> None:
    fake_table = FakeIcebergTable(
        pa.table(
            {
                "dataset_id": ["ds-1"],
                "image_id": ["img-1"],
            }
        )
    )
    catalog = FakeCatalog({("bioimage", "image_assets"): fake_table})

    result = catalog_table_to_arrow(
        catalog,
        "bioimage",
        "image_assets",
        scan_options=CatalogScanOptions(columns="dataset_id"),
    )

    assert result.to_pydict() == {"dataset_id": ["ds-1"]}
    assert fake_table.calls[0]["selected_fields"] == ("dataset_id",)


def test_join_catalog_image_assets_with_profiles(
    monkeypatch: MonkeyPatch,
) -> None:
    image_assets_table = FakeIcebergTable(
        pa.table(
            {
                "dataset_id": ["ds-1"],
                "image_id": ["img-1"],
                "array_path": ["0"],
                "uri": ["data/example.zarr"],
            }
        )
    )
    chunk_index_table = FakeIcebergTable(
        pa.table(
            {
                "dataset_id": ["ds-1"],
                "image_id": ["img-1"],
                "array_path": ["0"],
                "chunk_key": ["0/0"],
                "chunk_coords_json": ["[0, 0]"],
                "byte_length": [1024],
            }
        )
    )
    catalog = FakeCatalog(
        {
            ("bioimage", "custom_image_assets"): image_assets_table,
            ("bioimage", "chunk_index"): chunk_index_table,
        }
    )
    profiles = pa.table(
        {
            "dataset_id": ["ds-1"],
            "image_id": ["img-1"],
            "cell_count": [42],
        }
    )
    expected = pa.table(
        {
            "dataset_id": ["ds-1"],
            "image_id": ["img-1"],
            "array_path": ["0"],
            "uri": ["data/example.zarr"],
            "cell_count": [42],
            "chunk_key": ["0/0"],
            "chunk_coords_json": ["[0, 0]"],
            "byte_length": [1024],
        }
    )

    def _fake_join_image_assets_with_profiles(
        image_assets: pa.Table,
        profiles_table: pa.Table,
        *,
        join_keys: tuple[str, ...] = ("dataset_id", "image_id"),
        chunk_index: pa.Table | None = None,
    ) -> pa.Table:
        assert image_assets.to_pydict()["uri"] == ["data/example.zarr"]
        assert profiles_table.to_pydict()["cell_count"] == [42]
        assert join_keys == ("dataset_id", "image_id")
        assert chunk_index is not None
        return expected

    monkeypatch.setattr(
        catalog_module,
        "join_image_assets_with_profiles",
        _fake_join_image_assets_with_profiles,
    )

    result = join_catalog_image_assets_with_profiles(
        catalog,
        "bioimage",
        profiles,
        image_assets_table="custom_image_assets",
        chunk_index_table="chunk_index",
        image_assets_scan_options=CatalogScanOptions(limit=1),
        chunk_index_scan_options=CatalogScanOptions(limit=1),
    )

    assert result.to_pydict() == {
        "dataset_id": ["ds-1"],
        "image_id": ["img-1"],
        "array_path": ["0"],
        "uri": ["data/example.zarr"],
        "cell_count": [42],
        "chunk_key": ["0/0"],
        "chunk_coords_json": ["[0, 0]"],
        "byte_length": [1024],
    }
    assert image_assets_table.calls[0]["limit"] == 1
    assert chunk_index_table.calls[0]["limit"] == 1


def test_join_catalog_image_assets_with_profiles_rejects_empty_join_keys() -> None:
    catalog = FakeCatalog({})
    profiles = pa.table({"dataset_id": ["ds-1"], "image_id": ["img-1"]})

    try:
        join_catalog_image_assets_with_profiles(
            catalog,
            "bioimage",
            profiles,
            join_keys=(),
        )
    except ValueError as exc:
        assert str(exc) == "join_keys must be a non-empty sequence of column names."
    else:  # pragma: no cover - defensive assertion style for clarity
        raise AssertionError("Expected ValueError for empty join_keys.")


def test_join_catalog_image_assets_with_profiles_accepts_string_join_key(
    monkeypatch: MonkeyPatch,
) -> None:
    image_assets_table = FakeIcebergTable(
        pa.table(
            {
                "dataset_id": ["ds-1"],
                "uri": ["data/example.zarr"],
            }
        )
    )
    catalog = FakeCatalog({("bioimage", "image_assets"): image_assets_table})
    profiles = pa.table({"dataset_id": ["ds-1"], "cell_count": [42]})

    def _fake_join_image_assets_with_profiles(
        image_assets: pa.Table,
        profiles_table: pa.Table,
        *,
        join_keys: tuple[str, ...] = ("dataset_id", "image_id"),
        chunk_index: pa.Table | None = None,
    ) -> pa.Table:
        assert image_assets.to_pydict()["uri"] == ["data/example.zarr"]
        assert profiles_table.to_pydict()["cell_count"] == [42]
        assert join_keys == ["dataset_id"]
        assert chunk_index is None
        return image_assets

    monkeypatch.setattr(
        catalog_module,
        "join_image_assets_with_profiles",
        _fake_join_image_assets_with_profiles,
    )

    result = join_catalog_image_assets_with_profiles(
        catalog,
        "bioimage",
        profiles,
        join_keys="dataset_id",
    )

    assert result.to_pydict()["dataset_id"] == ["ds-1"]
