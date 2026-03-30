# Workflow

## Current scope

The library currently supports:

- Scanning Zarr and OME-TIFF stores into canonical `ScanResult` objects
- Summarizing scan results into ergonomic `DatasetSummary` objects
- Publishing canonical `image_assets` rows to Iceberg with PyIceberg
- Publishing canonical `chunk_index` rows for chunked assets
- Validating profile-table schemas against the microscopy join contract
- Joining scanned image metadata to profile tables through a high-level API
- Optional DuckDB query helpers over canonical metadata tables
- Optional OME-Arrow helpers for Arrow-native image payload workflows
- Catalog-facing adapters for loading canonical metadata into query workflows

DuckDB integration remains optional and outside the core metadata model. The
library keeps execution concerns separate from scanning, validation, and
publishing so other query engines can be added later without reshaping the
core package.

Chunk index publishing is intentionally metadata-only. Assets without chunking
metadata simply produce zero `chunk_index` rows.

## Zarr v2 and v3

The package intentionally keeps one scan entry point for `.zarr` paths:
`scan_store(...)`.

- Local Zarr v2 stores are scanned through the `zarr` Python package
- Local Zarr v3 stores are scanned from `zarr.json` metadata
- Summaries expose the detected storage variant so users can inspect mixed
  catalogs without learning separate APIs
- The package dependency range now permits both Zarr 2 and Zarr 3 runtimes.
- Optional integrations such as OME-Arrow can coexist with the core scanner.

This keeps the package approachable while the wider ecosystem continues to
settle around Zarr v3 runtime support.

## Microscopy join contract

Required columns:

- `dataset_id`
- `image_id`

Recommended columns:

- `plate_id`
- `well_id`
- `site_id`

This validator is intentionally schema-focused. It checks whether a profile
table exposes the columns needed for stable joins against canonical image
metadata without taking on execution-engine responsibilities.

## CLI examples

```bash
iceberg-bioimage scan data/experiment.zarr
iceberg-bioimage summarize data/experiment.zarr
iceberg-bioimage register --catalog default --namespace bioimage data/experiment.zarr
iceberg-bioimage register --catalog default --namespace bioimage --publish-chunks data/experiment.zarr
iceberg-bioimage publish-chunks --catalog default --namespace bioimage data/experiment.zarr
iceberg-bioimage validate-contract data/cells.parquet
iceberg-bioimage join-profiles data/experiment.zarr data/cells.parquet --output joined.parquet
```

## Example workflows

- `examples/quickstart.py`: minimal registration and validation flow
- `examples/catalog_duckdb.py`: catalog-backed metadata joined to analysis rows
- `examples/synthetic_workflow.py`: self-contained local synthetic workflow
- `docs/src/examples/metadata-workflow.ipynb`: summary and join workflow in a
  notebook
