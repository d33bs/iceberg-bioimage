"""Tests for optional OME-Arrow integration helpers."""

from __future__ import annotations

import importlib
import sys

import pytest
from pytest import MonkeyPatch

from iceberg_bioimage.integrations.ome_arrow import create_ome_arrow, scan_ome_arrow


def test_create_ome_arrow_requires_optional_dependency(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.delitem(sys.modules, "ome_arrow", raising=False)
    original_import_module = importlib.import_module

    def _missing_import(name: str, package: str | None = None) -> object:
        if name == "ome_arrow":
            raise ImportError("ome_arrow is unavailable")
        return original_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", _missing_import)

    with pytest.raises(RuntimeError, match="optional ome-arrow extra"):
        create_ome_arrow("image.ome.tiff")


def test_scan_ome_arrow_requires_optional_dependency(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.delitem(sys.modules, "ome_arrow", raising=False)
    original_import_module = importlib.import_module

    def _missing_import(name: str, package: str | None = None) -> object:
        if name == "ome_arrow":
            raise ImportError("ome_arrow is unavailable")
        return original_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", _missing_import)

    with pytest.raises(RuntimeError, match="optional ome-arrow extra"):
        scan_ome_arrow("image.ome.parquet")
