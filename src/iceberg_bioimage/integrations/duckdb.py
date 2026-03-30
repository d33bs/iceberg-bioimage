"""Optional DuckDB query helpers for canonical metadata tables."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, TypeAlias

import pyarrow as pa

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection, DuckDBPyRelation

MetadataSource = str | Path | pa.Table | list[dict[str, object]]
DEFAULT_JOIN_KEYS = ("dataset_id", "image_id")
CHUNK_INDEX_FIELDS = ("chunk_key", "chunk_coords_json", "byte_length")
FilterValue: TypeAlias = str | int | float | bool
FilterClause: TypeAlias = tuple[str, str, FilterValue | None]
ALLOWED_FILTER_OPERATORS = {
    "=",
    "!=",
    "<",
    "<=",
    ">",
    ">=",
    "IS",
    "IS NOT",
}


def create_duckdb_connection(
    database: str = ":memory:",
    *,
    read_only: bool = False,
) -> DuckDBPyConnection:
    """Create a DuckDB connection.

    DuckDB is optional for this project. This helper isolates the import so the
    core package remains engine-neutral unless the user explicitly opts in.
    """

    duckdb = _require_duckdb()
    return duckdb.connect(database=database, read_only=read_only)


def query_metadata_table(
    source: MetadataSource,
    *,
    columns: Sequence[str] | None = None,
    filters: Sequence[FilterClause] | None = None,
    connection: DuckDBPyConnection | None = None,
) -> pa.Table:
    """Query a metadata table from a Parquet path, Arrow table, or row list."""

    duckdb_connection, owns_connection = _get_connection(connection)
    relation = _relation_for_source(duckdb_connection, source)

    if filters:
        relation = relation.filter(
            _build_filter_expression(tuple(relation.columns), filters)
        )

    if columns:
        relation = relation.project(", ".join(columns))

    result = _as_arrow_table(relation.arrow())
    if owns_connection:
        duckdb_connection.close()

    return result


def join_image_assets_with_profiles(
    image_assets: MetadataSource,
    profiles: MetadataSource,
    *,
    join_keys: Sequence[str] = DEFAULT_JOIN_KEYS,
    chunk_index: MetadataSource | None = None,
    connection: DuckDBPyConnection | None = None,
) -> pa.Table:
    """Join image metadata to a profile table using the canonical join keys."""

    duckdb_connection, owns_connection = _get_connection(connection)
    _register_source(duckdb_connection, "image_assets", image_assets)
    _register_source(duckdb_connection, "profiles", profiles)

    using_clause = ", ".join(join_keys)
    query = [
        "SELECT ia.*,",
        f"       p.* EXCLUDE ({using_clause})",
    ]

    if chunk_index is not None:
        _register_source(duckdb_connection, "chunk_index", chunk_index)
        query.append(
            "     , " + ", ".join(f"ci.{field}" for field in CHUNK_INDEX_FIELDS)
        )

    query.extend(
        [
            "FROM image_assets AS ia",
            f"INNER JOIN profiles AS p USING ({using_clause})",
        ]
    )

    if chunk_index is not None:
        query.append(
            "LEFT JOIN chunk_index AS ci "
            "ON ia.dataset_id = ci.dataset_id "
            "AND ia.image_id = ci.image_id "
            "AND ia.array_path IS NOT DISTINCT FROM ci.array_path"
        )

    result = _as_arrow_table(duckdb_connection.execute("\n".join(query)).arrow())
    if owns_connection:
        duckdb_connection.close()

    return result


def _get_connection(
    connection: DuckDBPyConnection | None,
) -> tuple[DuckDBPyConnection, bool]:
    if connection is not None:
        return connection, False

    return create_duckdb_connection(), True


def _relation_for_source(
    connection: DuckDBPyConnection,
    source: MetadataSource,
) -> DuckDBPyRelation:
    if isinstance(source, (str, Path)):
        return connection.from_parquet(str(source))

    if isinstance(source, list):
        return connection.from_arrow(pa.Table.from_pylist(source))

    return connection.from_arrow(source)


def _register_source(
    connection: DuckDBPyConnection,
    name: str,
    source: MetadataSource,
) -> None:
    if isinstance(source, (str, Path)):
        connection.from_parquet(str(source)).create_view(name, replace=True)
        return

    if isinstance(source, list):
        connection.register(name, pa.Table.from_pylist(source))
        return

    connection.register(name, source)


def _require_duckdb() -> object:
    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover - exercised without group
        raise RuntimeError(
            "DuckDB helpers require the optional duckdb dependency group. "
            "Install it with `uv sync --group duckdb` or `uv run --group duckdb`."
        ) from exc

    return duckdb


def _as_arrow_table(result: pa.Table | pa.RecordBatchReader) -> pa.Table:
    if isinstance(result, pa.RecordBatchReader):
        return result.read_all()

    return result


def _build_filter_expression(
    available_columns: Sequence[str],
    filters: Sequence[FilterClause],
) -> str:
    column_names = set(available_columns)
    expressions: list[str] = []

    for column, operator, value in filters:
        if column not in column_names:
            raise ValueError(
                f"Unknown filter column {column!r}. "
                f"Available columns: {sorted(column_names)!r}."
            )

        normalized_operator = operator.strip().upper()
        if normalized_operator not in ALLOWED_FILTER_OPERATORS:
            raise ValueError(
                f"Unsupported filter operator {operator!r}. "
                f"Allowed operators: {sorted(ALLOWED_FILTER_OPERATORS)!r}."
            )

        if value is None and normalized_operator not in {"IS", "IS NOT"}:
            raise ValueError("None filter values require 'IS' or 'IS NOT' operators.")

        expressions.append(
            f"{_quote_identifier(column)} {normalized_operator} {_quote_literal(value)}"
        )

    return " AND ".join(expressions)


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _quote_literal(value: FilterValue | None) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"

    return str(value)
