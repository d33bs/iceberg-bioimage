# Cytomining Workflow

`iceberg-bioimage` treats Cytomining interoperability as a primary workflow.

The package supports two common paths:

1. Create a Cytomining-compatible Parquet warehouse root directly from image
   stores such as Zarr or OME-TIFF.
1. Materialize a Cytomining-compatible Parquet warehouse root from existing
   Iceberg metadata tables.

These exports are designed to be useful to tools like `pycytominer` while
keeping image scanning, metadata canonicalization, and namespace handling in
this repository.

Each warehouse root also carries a `warehouse_manifest.json` file so appended
tables are described by role, join keys, provenance, and columns rather than
only by directory name.

## Warehouse layout

The Parquet warehouse root can contain:

- `images/image_assets/`
- `images/chunk_index/`
- `profiles/joined_profiles/`

`images.image_assets` is the base metadata table.

`images.chunk_index` is optional and only contains rows for chunked assets.

`profiles.joined_profiles` is optional and is written when a profile table is
provided.

## Export From Image Stores

Use this when a Cytomining project starts from raw image data and wants a
Parquet warehouse root immediately:

```python
from iceberg_bioimage import export_store_to_cytomining_warehouse

result = export_store_to_cytomining_warehouse(
    "data/experiment.zarr",
    "warehouse-root",
    profiles="data/profiles.parquet",
    profile_dataset_id="experiment",
)
print(result.to_dict())
```

CLI:

```bash
iceberg-bioimage export-cytomining \
  --warehouse-root warehouse-root \
  --profiles data/profiles.parquet \
  --profile-dataset-id experiment \
  data/experiment.zarr
```

## Export From Existing Iceberg Metadata

Use this when a project already has `image_assets` and `chunk_index` in an
Iceberg catalog and wants a Cytomining warehouse root for downstream tools:

```python
from iceberg_bioimage import export_catalog_to_cytomining_warehouse

result = export_catalog_to_cytomining_warehouse(
    "default",
    "bioimage.cytotable",
    "warehouse-root",
    profiles="data/profiles.parquet",
    profile_dataset_id="experiment",
)
print(result.to_dict())
```

CLI:

```bash
iceberg-bioimage export-cytomining-catalog \
  --catalog default \
  --namespace bioimage.cytotable \
  --warehouse-root warehouse-root \
  --profiles data/profiles.parquet \
  --profile-dataset-id experiment
```

## Existing Warehouse Roots

Both export helpers support:

- `mode="overwrite"` for replacing target tables
- `mode="append"` for adding additional Parquet parts to an existing warehouse

`mode="overwrite"` is table-scoped and does not remove unrelated table
directories in the same warehouse root.

This makes it possible to incrementally add datasets from multiple assays or
plates into the same Cytomining-oriented warehouse root.

## ExampleHuman To Cytomining Workflow

One useful pattern for Cytomining projects is:

1. use `CytoTable` to convert ExampleHuman-style measurement outputs into an
   Iceberg-backed warehouse
1. use `iceberg-bioimage` to materialize that metadata into a Cytomining
   Parquet warehouse root
1. append downstream `pycytominer` outputs as named warehouse tables
1. append downstream `coSMicQC` outputs as named warehouse tables

That looks like:

```bash
# 1. External step: build or update the CytoTable/Iceberg warehouse
#    from ExampleHuman measurement outputs.

# 2. Export the Iceberg-backed metadata into a Cytomining warehouse root.
iceberg-bioimage export-cytomining-catalog \
  --catalog default \
  --namespace bioimage.cytotable \
  --warehouse-root warehouse-root \
  --profiles data/examplehuman_profiles.parquet \
  --profile-dataset-id ExampleHuman

# 3. Append a pycytominer output table.
iceberg-bioimage export-cytomining-profiles \
  --warehouse-root warehouse-root \
  --table-name pycytominer_profiles \
  --profile-dataset-id ExampleHuman \
  data/pycytominer_output.parquet

# 4. Append a coSMicQC output table.
iceberg-bioimage export-cytomining-profiles \
  --warehouse-root warehouse-root \
  --table-name cosmicqc_profiles \
  --role quality_control \
  --profile-dataset-id ExampleHuman \
  data/cosmicqc_output.parquet
```

After those steps, the same warehouse root can contain:

- `images/image_assets/`
- `images/chunk_index/`
- `profiles/joined_profiles/`
- `profiles/pycytominer_profiles/`
- `quality_control/cosmicqc_profiles/`

This keeps the image metadata and downstream Cytomining analysis outputs in one
portable Parquet layout.

## Generic Table Export

For Cytomining projects with outputs that do not fit one narrow static
convention, use the generic table export API and record the table role in the
manifest:

```python
import pyarrow as pa

from iceberg_bioimage import export_table_to_cytomining_warehouse

result = export_table_to_cytomining_warehouse(
    pa.table(
        {
            "dataset_id": ["ExampleHuman"],
            "image_id": ["ExampleHuman:0"],
            "embedding_0": [0.1],
            "embedding_1": [0.2],
        }
    ),
    "warehouse-root",
    table_name="embeddings",
    role="embeddings",
    join_keys=["dataset_id", "image_id"],
    source_type="custom",
    source_ref="my-embedding-pipeline",
)
print(result.to_dict())
```

This is the intended scaling path for additional Cytomining outputs such as:

- embeddings
- QC summaries
- annotations
- segmentation metrics
- experiment-level reports

## Cytomining Metadata Compatibility

Profile-table compatibility is designed for common Cytomining conventions.

The join and export paths recognize aliases such as:

- `Metadata_dataset_id`
- `Metadata_ImageID`
- `Metadata_Plate`
- `Metadata_Well`
- `Metadata_Site`

If a profile table does not include `dataset_id` but all rows belong to a
single dataset, pass `profile_dataset_id`.

If your project uses custom column names, load aliases from TOML and pass them
into the profile export path:

```toml
[microscopy.aliases]
dataset_id = ["ProjectID"]
image_id = ["ImageKey"]
well_id = ["WellName"]
```

```python
from iceberg_bioimage import (
    export_profiles_to_cytomining_warehouse,
    load_profile_column_aliases,
)

aliases = load_profile_column_aliases("aliases.toml")
export_profiles_to_cytomining_warehouse(
    "data/custom_profiles.parquet",
    "warehouse-root",
    table_name="custom_profiles",
    alias_map=aliases,
)
```

## OME-Arrow and Other Columns

This repository does not try to reinterpret arbitrary non-tabular payload
columns. If a profile table or joined output includes OME-Arrow payload-related
columns, they are preserved in the Parquet export as long as the join keys and
the analyzable feature columns remain valid.
