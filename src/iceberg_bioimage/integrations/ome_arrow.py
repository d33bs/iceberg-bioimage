"""Optional OME-Arrow integration helpers."""

from __future__ import annotations

import importlib
from typing import Any


def create_ome_arrow(data: Any, **kwargs: Any) -> object:  # noqa: ANN401
    """Create an ``ome_arrow.OMEArrow`` object when the optional extra is installed."""

    ome_arrow = _require_ome_arrow()
    return ome_arrow.OMEArrow(data=data, **kwargs)


def scan_ome_arrow(data: str, **kwargs: Any) -> object:  # noqa: ANN401
    """Create a lazy ``ome_arrow.OMEArrow`` scan plan for tabular image sources."""

    ome_arrow = _require_ome_arrow()
    return ome_arrow.OMEArrow.scan(data=data, **kwargs)


def _require_ome_arrow() -> object:
    try:
        ome_arrow = importlib.import_module("ome_arrow")
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise RuntimeError(
            "OME-Arrow helpers require the optional ome-arrow extra. "
            "Install it with `pip install 'iceberg-bioimage[ome-arrow]'` "
            "or `uv sync --group ome-arrow`."
        ) from exc

    return ome_arrow
