"""Command-line interface for iceberg_bioimage."""

from __future__ import annotations

import argparse
import json
import sys

import pyarrow.parquet as pq

from iceberg_bioimage.api import (
    ingest_stores_to_warehouse,
    join_profiles_with_store,
    register_store,
    scan_store,
    summarize_store,
)
from iceberg_bioimage.integrations.cytomining import (
    DEFAULT_CHUNK_INDEX_TABLE,
    DEFAULT_IMAGE_ASSETS_TABLE,
    export_catalog_to_cytomining_warehouse,
    export_profiles_to_cytomining_warehouse,
    export_store_to_cytomining_warehouse,
)
from iceberg_bioimage.models.scan_result import (
    ContractValidationResult,
    DatasetSummary,
    ScanResult,
)
from iceberg_bioimage.publishing.chunk_index import publish_chunk_index
from iceberg_bioimage.validation.contracts import validate_microscopy_profile_table


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""

    parser = argparse.ArgumentParser(prog="iceberg-bioimage")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_parser = subparsers.add_parser(
        "scan",
        help="Scan a dataset and print a summary.",
    )
    scan_parser.add_argument("uri")
    scan_parser.add_argument(
        "--json",
        action="store_true",
        help="Print the full serialized ScanResult instead of a summary.",
    )
    scan_parser.set_defaults(handler=_handle_scan)

    summarize_parser = subparsers.add_parser(
        "summarize",
        help="Scan a dataset and print an aggregated summary.",
    )
    summarize_parser.add_argument("uri")
    summarize_parser.add_argument(
        "--json",
        action="store_true",
        help="Print the full serialized summary instead of a text summary.",
    )
    summarize_parser.set_defaults(handler=_handle_summarize)

    register_parser = subparsers.add_parser(
        "register",
        help="Publish scan metadata into an Iceberg image_assets table.",
    )
    register_parser.add_argument("uri")
    register_parser.add_argument("--catalog", required=True)
    register_parser.add_argument("--namespace", required=True)
    register_parser.add_argument("--table-name", default="image_assets")
    register_parser.add_argument(
        "--publish-chunks",
        action="store_true",
        help="Also publish derived chunk metadata to the chunk_index table.",
    )
    register_parser.add_argument("--chunk-table-name", default="chunk_index")
    register_parser.set_defaults(handler=_handle_register)

    ingest_parser = subparsers.add_parser(
        "ingest",
        help=(
            "Ingest one or more existing datasets into a "
            "Cytotable-compatible warehouse."
        ),
    )
    ingest_parser.add_argument("uris", nargs="+")
    ingest_parser.add_argument("--catalog", required=True)
    ingest_parser.add_argument("--namespace", required=True)
    ingest_parser.add_argument("--table-name", default="image_assets")
    ingest_parser.add_argument(
        "--chunk-table-name",
        default="chunk_index",
        help="Chunk-index table name. Use --skip-chunks to disable chunk ingestion.",
    )
    ingest_parser.add_argument(
        "--skip-chunks",
        action="store_true",
        help="Skip chunk-index ingestion and publish only image_assets rows.",
    )
    ingest_parser.set_defaults(handler=_handle_ingest)

    cytomining_parser = subparsers.add_parser(
        "export-cytomining",
        help="Export a dataset into a Parquet Cytomining warehouse root.",
    )
    cytomining_parser.add_argument("uri")
    cytomining_parser.add_argument("--warehouse-root", required=True)
    cytomining_parser.add_argument("--profiles")
    cytomining_parser.add_argument(
        "--skip-chunks",
        action="store_true",
        help="Skip chunk-index export and write only image_assets or joined_profiles.",
    )
    cytomining_parser.add_argument(
        "--profile-dataset-id",
        help=(
            "Inject dataset_id for profile tables that only carry "
            "Cytomining Metadata_* columns."
        ),
    )
    cytomining_parser.add_argument(
        "--mode",
        choices=("overwrite", "append"),
        default="overwrite",
    )
    cytomining_parser.set_defaults(handler=_handle_export_cytomining)

    cytomining_catalog_parser = subparsers.add_parser(
        "export-cytomining-catalog",
        help="Export catalog-backed metadata into a Parquet Cytomining warehouse root.",
    )
    cytomining_catalog_parser.add_argument("--catalog", required=True)
    cytomining_catalog_parser.add_argument("--namespace", required=True)
    cytomining_catalog_parser.add_argument("--warehouse-root", required=True)
    cytomining_catalog_parser.add_argument("--profiles")
    cytomining_catalog_parser.add_argument(
        "--image-assets-table",
        default=DEFAULT_IMAGE_ASSETS_TABLE,
    )
    cytomining_catalog_parser.add_argument(
        "--chunk-index-table",
        default=DEFAULT_CHUNK_INDEX_TABLE,
    )
    cytomining_catalog_parser.add_argument(
        "--skip-chunks",
        action="store_true",
        help="Skip chunk-index export and write only image_assets or joined_profiles.",
    )
    cytomining_catalog_parser.add_argument(
        "--profile-dataset-id",
        help=(
            "Inject dataset_id for profile tables that only carry "
            "Cytomining Metadata_* columns."
        ),
    )
    cytomining_catalog_parser.add_argument(
        "--mode",
        choices=("overwrite", "append"),
        default="overwrite",
    )
    cytomining_catalog_parser.set_defaults(handler=_handle_export_cytomining_catalog)

    cytomining_profiles_parser = subparsers.add_parser(
        "export-cytomining-profiles",
        help="Append a Cytomining profile table into a Parquet warehouse root.",
    )
    cytomining_profiles_parser.add_argument("profiles")
    cytomining_profiles_parser.add_argument("--warehouse-root", required=True)
    cytomining_profiles_parser.add_argument(
        "--table-name",
        default="profiles",
    )
    cytomining_profiles_parser.add_argument(
        "--role",
        default="profiles",
        help=(
            "Manifest role for the exported table (for example profiles or "
            "quality_control)."
        ),
    )
    cytomining_profiles_parser.add_argument(
        "--profile-dataset-id",
        help=(
            "Inject dataset_id for profile tables that only carry "
            "Cytomining Metadata_* columns."
        ),
    )
    cytomining_profiles_parser.add_argument(
        "--mode",
        choices=("overwrite", "append"),
        default="append",
    )
    cytomining_profiles_parser.set_defaults(handler=_handle_export_cytomining_profiles)

    validate_parser = subparsers.add_parser(
        "validate-contract",
        help="Validate a profile table against the microscopy join contract.",
    )
    validate_parser.add_argument("profile_table")
    validate_parser.add_argument(
        "--json",
        action="store_true",
        help="Print the full validation result as JSON.",
    )
    validate_parser.set_defaults(handler=_handle_validate_contract)

    chunk_parser = subparsers.add_parser(
        "publish-chunks",
        help="Publish derived chunk metadata into an Iceberg chunk_index table.",
    )
    chunk_parser.add_argument("uri")
    chunk_parser.add_argument("--catalog", required=True)
    chunk_parser.add_argument("--namespace", required=True)
    chunk_parser.add_argument("--table-name", default="chunk_index")
    chunk_parser.set_defaults(handler=_handle_publish_chunks)

    join_parser = subparsers.add_parser(
        "join-profiles",
        help="Join a scanned image dataset to a profile table and write Parquet.",
    )
    join_parser.add_argument("uri")
    join_parser.add_argument("profile_table")
    join_parser.add_argument("--output", required=True)
    join_parser.add_argument(
        "--include-chunks",
        action="store_true",
        help="Include chunk_index rows in the join when available.",
    )
    join_parser.add_argument(
        "--profile-dataset-id",
        help=(
            "Inject dataset_id for profile tables that only carry "
            "pycytominer/coSMicQC Metadata_* columns."
        ),
    )
    join_parser.set_defaults(handler=_handle_join_profiles)

    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI."""

    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args))
    except (RuntimeError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


def _handle_scan(args: argparse.Namespace) -> int:
    scan_result = scan_store(args.uri)

    if args.json:
        print(scan_result.to_json(indent=2, sort_keys=True))
        return 0

    print(_scan_summary(scan_result))
    return 0


def _handle_summarize(args: argparse.Namespace) -> int:
    summary = summarize_store(args.uri)

    if args.json:
        print(summary.to_json(indent=2, sort_keys=True))
        return 0

    print(_dataset_summary(summary))
    return 0


def _handle_register(args: argparse.Namespace) -> int:
    registration = register_store(
        args.uri,
        args.catalog,
        args.namespace,
        image_assets_table=args.table_name,
        chunk_index_table=(args.chunk_table_name if args.publish_chunks else None),
    )
    payload = {
        "catalog": args.catalog,
        "namespace": args.namespace,
        "image_assets_table": args.table_name,
        "image_assets_rows_published": registration.image_assets_rows_published,
        "chunk_rows_published": registration.chunk_rows_published,
        "source_uri": args.uri,
    }
    if args.publish_chunks:
        payload["chunk_table_name"] = args.chunk_table_name

    print(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _handle_validate_contract(args: argparse.Namespace) -> int:
    result = validate_microscopy_profile_table(args.profile_table)

    if args.json:
        print(result.to_json(indent=2, sort_keys=True))
    else:
        print(_contract_summary(result))

    return 0 if result.is_valid else 1


def _handle_ingest(args: argparse.Namespace) -> int:
    result = ingest_stores_to_warehouse(
        args.uris,
        args.catalog,
        args.namespace,
        image_assets_table=args.table_name,
        chunk_index_table=(None if args.skip_chunks else args.chunk_table_name),
    )
    print(result.to_json(indent=2, sort_keys=True))
    return 0


def _handle_export_cytomining(args: argparse.Namespace) -> int:
    result = export_store_to_cytomining_warehouse(
        args.uri,
        args.warehouse_root,
        profiles=args.profiles,
        include_chunks=not args.skip_chunks,
        profile_dataset_id=args.profile_dataset_id,
        mode=args.mode,
    )
    print(result.to_json(indent=2, sort_keys=True))
    return 0


def _handle_export_cytomining_catalog(args: argparse.Namespace) -> int:
    result = export_catalog_to_cytomining_warehouse(
        args.catalog,
        args.namespace,
        args.warehouse_root,
        profiles=args.profiles,
        image_assets_table_name=args.image_assets_table,
        chunk_index_table_name=(None if args.skip_chunks else args.chunk_index_table),
        profile_dataset_id=args.profile_dataset_id,
        mode=args.mode,
    )
    print(result.to_json(indent=2, sort_keys=True))
    return 0


def _handle_export_cytomining_profiles(args: argparse.Namespace) -> int:
    result = export_profiles_to_cytomining_warehouse(
        args.profiles,
        args.warehouse_root,
        table_name=args.table_name,
        role=args.role,
        profile_dataset_id=args.profile_dataset_id,
        mode=args.mode,
    )
    print(result.to_json(indent=2, sort_keys=True))
    return 0


def _handle_publish_chunks(args: argparse.Namespace) -> int:
    scan_result = scan_store(args.uri)
    row_count = publish_chunk_index(
        catalog=args.catalog,
        namespace=args.namespace,
        table_name=args.table_name,
        scan_result=scan_result,
    )
    print(
        json.dumps(
            {
                "catalog": args.catalog,
                "namespace": args.namespace,
                "table_name": args.table_name,
                "rows_published": row_count,
                "source_uri": args.uri,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _handle_join_profiles(args: argparse.Namespace) -> int:
    joined = join_profiles_with_store(
        args.uri,
        args.profile_table,
        include_chunks=args.include_chunks,
        profile_dataset_id=args.profile_dataset_id,
    )
    pq.write_table(joined, args.output)
    print(
        json.dumps(
            {
                "source_uri": args.uri,
                "profile_table": args.profile_table,
                "output": args.output,
                "rows_written": joined.num_rows,
                "columns": list(joined.column_names),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _scan_summary(scan_result: ScanResult) -> str:
    lines = [
        f"source_uri: {scan_result.source_uri}",
        f"format_family: {scan_result.format_family}",
        f"image_assets: {len(scan_result.image_assets)}",
    ]

    for asset in scan_result.image_assets:
        label = asset.array_path or "<root>"
        lines.append(
            f"- {label}: shape={asset.shape} dtype={asset.dtype}"
            + (f" chunks={asset.chunk_shape}" if asset.chunk_shape else "")
        )

    if scan_result.warnings:
        lines.append("warnings:")
        lines.extend(f"- {warning}" for warning in scan_result.warnings)

    return "\n".join(lines)


def _contract_summary(result: ContractValidationResult) -> str:
    lines = [
        f"target: {result.target}",
        f"is_valid: {result.is_valid}",
        f"required_columns: {', '.join(result.required_columns)}",
        f"recommended_columns: {', '.join(result.recommended_columns)}",
    ]

    if result.missing_required_columns:
        lines.append(
            "missing_required_columns: " + ", ".join(result.missing_required_columns)
        )

    if result.missing_recommended_columns:
        lines.append(
            "missing_recommended_columns: "
            + ", ".join(result.missing_recommended_columns)
        )

    if result.warnings:
        lines.append("warnings:")
        lines.extend(f"- {warning}" for warning in result.warnings)

    return "\n".join(lines)


def _dataset_summary(summary: DatasetSummary) -> str:
    lines = [
        f"source_uri: {summary.source_uri}",
        f"format_family: {summary.format_family}",
        f"image_asset_count: {summary.image_asset_count}",
        f"chunked_asset_count: {summary.chunked_asset_count}",
        f"dtypes: {', '.join(summary.dtypes)}",
    ]

    if summary.axes:
        lines.append(f"axes: {', '.join(summary.axes)}")
    if summary.channel_counts:
        lines.append(
            "channel_counts: "
            + ", ".join(str(value) for value in summary.channel_counts)
        )
    if summary.storage_variants:
        lines.append("storage_variants: " + ", ".join(summary.storage_variants))
    if summary.array_paths:
        lines.append("array_paths:")
        lines.extend(f"- {path}" for path in summary.array_paths)
    if summary.warnings:
        lines.append("warnings:")
        lines.extend(f"- {warning}" for warning in summary.warnings)

    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
