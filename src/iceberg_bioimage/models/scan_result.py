"""Serializable canonical scan models."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class ImageAsset:
    """Canonical representation of one discovered image asset."""

    uri: str
    shape: list[int]
    dtype: str
    array_path: str | None = None
    chunk_shape: list[int] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    image_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""

        return asdict(self)


@dataclass(slots=True)
class ScanResult:
    """Canonical scan output shared across adapters and publishers."""

    source_uri: str
    format_family: str
    image_assets: list[ImageAsset]
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""

        return {
            "source_uri": self.source_uri,
            "format_family": self.format_family,
            "image_assets": [asset.to_dict() for asset in self.image_assets],
            "warnings": list(self.warnings),
        }

    def to_json(self, **json_kwargs: Any) -> str:  # noqa: ANN401
        """Serialize the scan result to JSON."""

        return json.dumps(self.to_dict(), **json_kwargs)


@dataclass(slots=True)
class DatasetSummary:
    """User-facing summary of a scanned dataset."""

    source_uri: str
    format_family: str
    image_asset_count: int
    chunked_asset_count: int
    array_paths: list[str]
    dtypes: list[str]
    shapes: list[list[int]]
    axes: list[str]
    channel_counts: list[int]
    storage_variants: list[str]
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""

        return {
            "source_uri": self.source_uri,
            "format_family": self.format_family,
            "image_asset_count": self.image_asset_count,
            "chunked_asset_count": self.chunked_asset_count,
            "array_paths": list(self.array_paths),
            "dtypes": list(self.dtypes),
            "shapes": [list(shape) for shape in self.shapes],
            "axes": list(self.axes),
            "channel_counts": list(self.channel_counts),
            "storage_variants": list(self.storage_variants),
            "warnings": list(self.warnings),
        }

    def to_json(self, **json_kwargs: Any) -> str:  # noqa: ANN401
        """Serialize the dataset summary to JSON."""

        return json.dumps(self.to_dict(), **json_kwargs)


@dataclass(slots=True)
class ContractValidationResult:
    """Serializable result for schema-level contract validation."""

    target: str
    present_columns: list[str]
    required_columns: list[str]
    recommended_columns: list[str]
    missing_required_columns: list[str]
    missing_recommended_columns: list[str]
    warnings: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """Return whether all required columns are present."""

        return not self.missing_required_columns

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""

        return {
            "target": self.target,
            "present_columns": list(self.present_columns),
            "required_columns": list(self.required_columns),
            "recommended_columns": list(self.recommended_columns),
            "missing_required_columns": list(self.missing_required_columns),
            "missing_recommended_columns": list(self.missing_recommended_columns),
            "warnings": list(self.warnings),
            "is_valid": self.is_valid,
        }

    def to_json(self, **json_kwargs: Any) -> str:  # noqa: ANN401
        """Serialize the validation result to JSON."""

        return json.dumps(self.to_dict(), **json_kwargs)


@dataclass(slots=True)
class RegistrationResult:
    """Serializable result for a metadata registration workflow."""

    source_uri: str
    image_assets_rows_published: int
    chunk_rows_published: int

    def to_dict(self) -> dict[str, int | str]:
        """Return a JSON-serializable representation."""

        return {
            "source_uri": self.source_uri,
            "image_assets_rows_published": self.image_assets_rows_published,
            "chunk_rows_published": self.chunk_rows_published,
        }

    def to_json(self, **json_kwargs: Any) -> str:  # noqa: ANN401
        """Serialize the registration result to JSON."""

        return json.dumps(self.to_dict(), **json_kwargs)
