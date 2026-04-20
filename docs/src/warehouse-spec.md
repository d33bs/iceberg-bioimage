# Cytomining Warehouse Specification (RFC 2119)

## Status

Draft

Intended to be normative for Cytomining ecosystem implementations.
In this document, “normative” means statements using **MUST**, **MUST NOT**, **SHOULD**, and **MAY** define conformance requirements, while explanatory text and examples are informative.

## Terminology

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** are to be interpreted as described in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119).

## Scope

This specification defines a portable warehouse layout and naming contract for projects in and beyond the Cytomining ecosystem.

This specification defines warehouse structure, interoperability behavior, and manifest semantics.
Detailed per-table column schema constraints are defined in a companion schema specification for this project.

The structural core of this specification is intended for general computational bioimaging.
Cytomining-specific table conventions in this document can be treated as a theme layered on that core.

## Specification Versioning

This section defines how specification versions are declared and interpreted.

1. Every warehouse manifest **MUST** declare a `warehouse_spec_version`.
1. Spec versions **MUST** use semantic versioning (`MAJOR.MINOR.PATCH`).
   1. A `MAJOR` version change **MUST** indicate a breaking interoperability change.
   1. A `MINOR` version change **MUST** be backward-compatible for conforming readers.
   1. A `PATCH` version change **MUST NOT** change interoperability requirements, and it is limited to clarifications, errata, and non-normative fixes.
1. Readers **SHOULD** fail fast with a clear error when they cannot interpret a declared major version.

Implementations **SHOULD** document which sections of this specification they implement.

## Conformance

An implementation conforms to this specification if it satisfies all requirements marked **MUST** and **MUST NOT**.

## Namespaces and Tables

This section defines the required identifier format and filesystem mapping for tables.
In this specification, “Apache Iceberg-style” means using a namespace-qualified table identifier (`<namespace>.<table>`) rather than a flat table name.
See the [Apache Iceberg specification](https://iceberg.apache.org/spec/) for background on Iceberg table and namespace concepts.

1. A warehouse root **MUST** organize tables by Apache Iceberg-style namespace and table identifier.
1. A canonical namespace and table identifier **MUST** be represented as `<namespace>.<table>`.
1. On local filesystems, `<namespace>.<table>` **MUST** map to `<warehouse_root>/<namespace>/<table>/`.
1. On non-local filesystems or object storage (such as AWS S3 or Google Cloud Storage), `<namespace>.<table>` **MUST** map to a path or key prefix equivalent to `.../<namespace>/<table>/` beneath the warehouse root URI.

For clarity, this follows Apache Iceberg's namespace + table identifier model.
For example, `images.image_assets` means namespace: `images` and table: `image_assets`.
In a local exported warehouse, that identifier maps to `<warehouse_root>/images/image_assets/`.

## Namespaces

This section defines namespace expectations for profile, image, and quality-control tables.

### Canonical Namespaces

This subsection defines the default namespaces used by conforming implementations.

1. Profile-oriented tables **MUST** use the `profiles` namespace.
1. Image and image-metadata tables or data **MUST** use the `images` namespace.
1. Quality-control tables **MUST** use the `quality_control` namespace.
1. Implementations **SHOULD** reject ambiguous warehouse writes that omit namespace when multiple namespace defaults are possible.

### Image Namespace Data Conventions

This subsection defines how image-derived data should be represented under the `images` namespace.

1. Image assets stored within warehouses from OME-Zarr, OME-TIFF, and TIFF sources **MUST** be represented under the `images` namespace using canonical image metadata tables.
1. Source format differences, for example chunked OME-Zarr vs non-chunked TIFF, **MUST NOT** change namespace placement.
1. Canonical image metadata for all supported source formats **MUST** be stored in `images.image_assets`.
1. Chunk-derived metadata **MAY** be stored in `images.chunk_index`, and producers **MUST** write zero rows or omit the table when source assets do not expose chunk metadata.
1. Image identifiers **MUST** remain stable across source format variants so downstream joins to `profiles.*` and QC tables are format-agnostic.
1. Format-specific metadata fields **MAY** be included, and producers **SHOULD** preserve common canonical fields (`dataset_id`, `image_id`, shape/dtype metadata) for interoperability.

### Format Guidance (Informative)

This subsection is informative and does not add conformance requirements.
Parquet is the baseline default for tabular warehouse exports today.
For image workflows, OME-Arrow and OME-Zarr are currently the most consistent fallback choices across the Cytomining ecosystem.
Vortex is a promising future option for general tabular workloads once Apache Iceberg support is available.
Lance is a promising future option for random-access-heavy workloads, including large image-oriented datasets with chunk-level access patterns, and readers may refer to the [Lance paper](https://doi.org/10.48550/arXiv.2504.15247) for background.
Support status for these newer table formats in Apache Iceberg can be tracked at [apache/iceberg#12225](https://github.com/apache/iceberg/issues/12225).
Implementations may adopt additional formats over time, but they should preserve interoperability-first defaults when cross-tool compatibility is required.

## Tables

This section defines canonical table names and role semantics.

### Canonical Table Names and Descriptions

This subsection defines standard table identifiers for common warehouse content.

1. `profiles.joined_profiles` **MUST** be the canonical joined profile table name when a joined profile table is present.
1. `images.image_crops` **MUST** be used for per-object image crops when such a table is produced.
1. `images.source_images` **MUST** be used for source-image payload tables when such a table is produced.
1. `profiles.profile_with_images` **MAY** be produced as a derived analytical view when both profile and crop tables are present.
1. Post-processing profile outputs, for example pycytominer-derived normalized profiles, **MAY** be stored under `profiles.*` identifiers such as `profiles.normalized_profiles`, and they **SHOULD** use names that indicate the processing step they came from.
1. Additional project-specific tables **MAY** exist, but they **SHOULD** remain in a namespace consistent with their semantics, typically `profiles`, `images`, or `quality_control`.

### Role Vocabulary

A table role is the purpose label for a table.
The role is stored in each table entry in `warehouse_manifest.json` under the `role` field.
Consumers (warehouse readers) use this field to understand how a table should be interpreted without inferring behavior from table names alone.

1. Manifest table roles **MUST** come from a controlled vocabulary.
1. Standard roles **MUST** include at least the following values.
   - `profiles`
   - `image_assets`
   - `chunk_index`
   - `joined_profiles`
   - `quality_control`
1. Standard optional roles **MAY** include the following values.
   - `image_crops`
   - `source_images`
   - `embeddings`
   - `annotations`
   - `reports`
1. In this specification, `profiles` means tabular feature measurements intended for direct profile-level analysis and joins.
1. In this specification, `embeddings` means transformed latent representations derived from upstream data, which may come from profile features or image models.
1. Embedding tables **MAY** be classified as either `profiles` or `embeddings` depending on project semantics.
1. Producers **SHOULD** use the `profiles` namespace when embeddings are treated as the primary profile matrix for downstream analysis and joins.
1. Producers **SHOULD** use the `embeddings` namespace when they want to preserve explicit distinction from standard feature-profile outputs.
1. Project-specific roles **MAY** be added, but producers **MUST** document their semantics.

### Profile Tables

A profile table stores quantitative measurements for a defined biological object or analysis level, such as organoid-level, image-level, cell-level, or nucleus-level data.

We provide the following definitions to help describe profile specifications below:

- A biological object is the measured entity represented by a profile row, such as a cell, nucleus, organoid, or image-level aggregate.
- A compartment is a specific biological region or context for measurement, such as nuclei, cells, or cytoplasm.

1. Warehouses **MAY** contain multiple profile tables at different biological or analytical levels, including organoid-level, image-level, and object/compartment-level tables.
1. Profile tables **MUST** be stored distinctly based on what biological object the records represent, for example `profiles.organoid_profiles`, `profiles.nuclei_profiles`, or `profiles.joined_profiles`.
1. Profile tables **MUST NOT** rely on implicit mixed biological object rows in one table (all records per table represent the same kind of biological object).
1. Every profile table, regardless of what biological object is represented, **MUST** include unique object and image identifiers.
1. Profile tables **SHOULD** include explicit metadata, such as `profile_level` (for example `organoid`, `image`, `object`) and, when relevant, `compartment` (for example `cells`, `nuclei`, `cytoplasm`).
1. Producers (warehouse writers) **MUST** document joins within manifest metadata via `role` and **SHOULD** include biological object or compartment-indicating table names.

### Quality Control Tables

A quality control (QC) table records pass/fail flags or scores used to keep, filter, or exclude profile rows for downstream analysis.

1. Warehouses **MAY** contain multiple quality control (QC) datasets.
1. QC datasets intended to filter profiles, for example [coSMicQC](https://github.com/cytomining/coSMicQC) outputs, **MUST** be stored as quality-control namespace tables, for example under `quality_control.<table_name>`.
1. A QC table used for object-level filtering **SHOULD** include object-level identifiers when available.
1. A QC filtering indicator **SHOULD** be representable as at least a boolean column.
1. When a QC table is exported, its manifest `role` **MUST** identify the table as quality-control data, for example `quality_control` or a project-specific QC role.

## Manifest

This section defines the required structure and semantics of `warehouse_manifest.json`.

1. Warehouse roots **MUST** include `warehouse_manifest.json`.
1. Manifest entries **MUST** record table identifier as the canonical dotted name (`<namespace>.<table>`).
1. Manifest entries **MUST** include the following fields: `table_name`, `role`, `format`, `join_keys`, and `columns`.
1. Table names **MUST** be unique within a manifest.
1. Metadata **MUST** include enough information for a reader to determine role, analysis level, and profile compatibility without inspecting table contents.

## Independent and External Writes

This section defines constraints for modifications made by tools outside this package.

1. Data **MAY** be added to an existing warehouse by tools other than this package.
1. Such tools **MUST** preserve canonical table identifiers, namespace-path mapping, and manifest requirements defined by this specification.
1. Updates to `warehouse_manifest.json` **MUST** reflect added, replaced, or removed tables before declaring the warehouse ready for shared consumption.
1. Staging of external writes **SHOULD** be atomic, or as close as practical, so readers do not observe partially updated table and manifest state.
1. Appending data to an existing table **MUST NOT** silently change declared role or analysis-level semantics for that table.
1. Replacing a table in-place **MUST** preserve identifier stability (`<namespace>.<table>`) and **SHOULD** preserve role semantics unless a documented migration is performed.
1. If an external write changes analysis level, role, or join behavior, the writer **MUST** treat it as a compatibility-affecting change and **SHOULD** communicate updates with stakeholders.
1. External writers **SHOULD** record provenance in manifest metadata, for example source workflow or producer identity, so downstream tools can audit table origin.
1. After external modification, maintainers **SHOULD** run conformance validation before distributing or relying on the updated warehouse.
1. Consumers **MUST NOT** assume that all tables were produced by one package, and they **MUST** rely on manifest semantics and conformance rules.
