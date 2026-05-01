# Getting Started

## Choose A Path

Use this table to pick the first workflow:

| Goal                                                                 | Use this path                                                | Requires catalog config |
| -------------------------------------------------------------------- | ------------------------------------------------------------ | ----------------------- |
| Build a Cytomining Parquet warehouse from image files                | `export-cytomining` / `export_store_to_cytomining_warehouse` | No                      |
| Validate profile schemas and join keys                               | `validate-contract` / `validate_microscopy_profile_table`    | No                      |
| Validate warehouse manifest and layout conformance                   | `validate-warehouse` / `validate_warehouse_manifest`         | No                      |
| Publish canonical metadata to Iceberg tables                         | `register`, `ingest`, `publish-chunks`                       | Yes                     |
| Read canonical metadata from Iceberg and export to Parquet warehouse | `export-cytomining-catalog`                                  | Yes                     |

## Install

Base install:

```bash
pip install iceberg-bioimage
```

Optional integrations:

```bash
pip install 'iceberg-bioimage[duckdb]'
pip install 'iceberg-bioimage[ome-arrow]'
```

If you use `uv`, the equivalent is:

```bash
uv sync
uv sync --group duckdb
uv sync --group ome-arrow
```

## Minimal Success Path (No Catalog)

Create a Parquet warehouse directly from an image store:

```bash
iceberg-bioimage export-cytomining \
  --warehouse-root warehouse-root \
  data/experiment.zarr
```

Validate a profile table join contract:

```bash
iceberg-bioimage validate-contract data/profiles.parquet
iceberg-bioimage validate-warehouse warehouse-root
```

## Catalog-Backed Path

Commands using `--catalog` require a configured PyIceberg catalog. For setup and examples, see [Catalog Setup](catalog-setup.md).

Recommended namespace for new projects:

- `bioimage.cytotable`

The publishing/read helpers also support namespace fallback for existing layouts where tables already exist under `bioimage`.

## Common Errors

- `DuckDB helpers require the optional duckdb dependency group`:
  install with `pip install 'iceberg-bioimage[duckdb]'` or `uv sync --group duckdb`.
- `Profiles do not satisfy the microscopy join contract`:
  run `iceberg-bioimage validate-contract ...` and provide `--profile-dataset-id` when `dataset_id` is missing but implied.
- `Missing table: ...` with catalog-backed joins/exports:
  confirm catalog name, namespace, and table names (`image_assets`, optional `chunk_index`).
