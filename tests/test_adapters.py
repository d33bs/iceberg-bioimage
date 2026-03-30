"""Adapter-specific regression tests."""

from __future__ import annotations

import json
from pathlib import Path

from iceberg_bioimage.adapters.ome_tiff import OMETiffAdapter
from iceberg_bioimage.adapters.zarr_v2 import TraversalContext, ZarrV2Adapter


def test_ome_tiff_image_id_strips_suffix_case_insensitively() -> None:
    adapter = OMETiffAdapter()

    assert adapter._image_id("/tmp/image.OME.TIFF", 0) == "image"


def test_zarr_image_id_strips_suffix_case_insensitively() -> None:
    adapter = ZarrV2Adapter()

    assert adapter._image_id("/tmp/data.ZARR", None) == "data"


def test_zarr_image_id_strips_ome_zarr_suffix() -> None:
    adapter = ZarrV2Adapter()

    assert adapter._image_id("/tmp/sample.OME.ZARR", None) == "sample"


def test_zarr_adapter_detects_local_v3_store(tmp_path: Path) -> None:
    store_path = tmp_path / "demo.zarr"
    store_path.mkdir()
    (store_path / "zarr.json").write_text(
        json.dumps({"zarr_format": 3, "node_type": "group"})
    )

    adapter = ZarrV2Adapter()

    assert adapter.can_handle(str(store_path))
    assert adapter._is_local_zarr_v3(str(store_path)) is True


def test_zarr_adapter_detects_local_v3_store_from_file_uri(tmp_path: Path) -> None:
    store_path = tmp_path / "demo.zarr"
    store_path.mkdir()
    (store_path / "zarr.json").write_text(
        json.dumps({"zarr_format": 3, "node_type": "group"})
    )

    adapter = ZarrV2Adapter()

    assert adapter._is_local_zarr_v3(store_path.resolve().as_uri()) is True


def test_zarr_adapter_collects_arrays_without_visititems() -> None:
    adapter = ZarrV2Adapter()
    image_assets = []

    class FakeArray:
        shape = (2, 3)
        dtype = "uint8"
        chunks = (1, 3)

    class FakeGroup:
        def __init__(self, mapping: dict[str, object]) -> None:
            self._mapping = mapping

        def keys(self) -> list[str]:
            return list(self._mapping)

        def __getitem__(self, key: str) -> object:
            return self._mapping[key]

    adapter._collect_group_arrays(
        node=FakeGroup({"0": FakeArray()}),
        context=TraversalContext(
            uri="/tmp/demo.zarr",
            image_assets=image_assets,
            root_attrs={},
        ),
    )

    assert [asset.array_path for asset in image_assets] == ["0"]
