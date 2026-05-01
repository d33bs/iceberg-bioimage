#!/usr/bin/env python3
"""Generate a LinkML ER diagram Mermaid file and rendered PNG.

This follows LinkML ER diagram generation guidance:
https://linkml.io/linkml/generators/erdiagram.html
"""

from __future__ import annotations

import argparse
import base64
import urllib.error
import urllib.request
import zlib
from pathlib import Path

from linkml.generators.erdiagramgen import ERDiagramGenerator
from linkml_runtime.utils.schemaview import SchemaView


def render_mermaid_png(mermaid_text: str, output_png: Path) -> None:
    """Render Mermaid text to PNG using Kroki."""
    req = urllib.request.Request(
        url="https://kroki.io/mermaid/png",
        data=mermaid_text.encode("utf-8"),
        headers={
            "Content-Type": "text/plain; charset=utf-8",
            "User-Agent": "Mozilla/5.0",
            "Accept": "image/png,image/*;q=0.8,*/*;q=0.5",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as response:
        png_bytes = response.read()
    output_png.write_bytes(png_bytes)


def render_plantuml_png(plantuml_text: str, output_png: Path) -> None:
    """Render PlantUML text to PNG using Kroki."""
    req = urllib.request.Request(
        url="https://kroki.io/plantuml/png",
        data=plantuml_text.encode("utf-8"),
        headers={
            "Content-Type": "text/plain; charset=utf-8",
            "User-Agent": "Mozilla/5.0",
            "Accept": "image/png,image/*;q=0.8,*/*;q=0.5",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as response:
        png_bytes = response.read()
    output_png.write_bytes(png_bytes)


def render_mermaid_png_fallback(mermaid_text: str, output_png: Path) -> None:
    """Render Mermaid text to PNG using mermaid.ink."""
    compressor = zlib.compressobj(level=9, wbits=-15)
    compressed = compressor.compress(mermaid_text.encode("utf-8")) + compressor.flush()
    payload = base64.urlsafe_b64encode(compressed).decode("ascii")
    url = f"https://mermaid.ink/img/pako:{payload}?type=png"
    req = urllib.request.Request(
        url=url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "image/png,image/*;q=0.8,*/*;q=0.5",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=60) as response:
        png_bytes = response.read()
    output_png.write_bytes(png_bytes)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("schema", type=Path, help="Path to LinkML schema YAML")
    parser.add_argument(
        "--outdir",
        type=Path,
        default=Path("."),
        help="Output directory",
    )
    args = parser.parse_args()

    args.outdir.mkdir(parents=True, exist_ok=True)
    stem = args.schema.stem

    # Per LinkML docs: generate Mermaid ERD content via erdiagram generator.
    mermaid = ERDiagramGenerator(str(args.schema), format="mermaid").serialize()
    plantuml = build_presentation_plantuml(args.schema)
    mermaid_path = args.outdir / f"{stem}.erdiagram.mmd"
    plantuml_path = args.outdir / f"{stem}.plantuml.puml"
    png_path = args.outdir / f"{stem}.erdiagram.png"
    plantuml_png_path = args.outdir / f"{stem}.plantuml.png"
    mermaid_path.write_text(mermaid, encoding="utf-8")
    plantuml_path.write_text(plantuml, encoding="utf-8")
    print(f"Wrote {mermaid_path}")
    print(f"Wrote {plantuml_path}")
    try:
        render_plantuml_png(plantuml, plantuml_png_path)
        print(f"Wrote {plantuml_png_path}")
    except urllib.error.URLError as err:
        raise SystemExit(
            "Failed to render PlantUML PNG via https://kroki.io. "
            f"PlantUML source was generated at {plantuml_path}. Error: {err}"
        ) from err

    compact_mermaid = ERDiagramGenerator(
        str(args.schema),
        format="mermaid",
        exclude_attributes=True,
    ).serialize()
    compact_path = args.outdir / f"{stem}.erdiagram.compact.mmd"

    # Prefer full ERD, but if parser/render services reject it, fallback to compact.
    for label, candidate in (("full", mermaid), ("compact", compact_mermaid)):
        try:
            render_mermaid_png(candidate, png_path)
            if label == "compact":
                compact_path.write_text(compact_mermaid, encoding="utf-8")
                print(f"Wrote {compact_path}")
                print(f"Wrote {png_path} (compact fallback)")
            else:
                print(f"Wrote {png_path}")
            return
        except urllib.error.URLError:
            pass
        try:
            render_mermaid_png_fallback(candidate, png_path)
            if label == "compact":
                compact_path.write_text(compact_mermaid, encoding="utf-8")
                print(f"Wrote {compact_path}")
                print(f"Wrote {png_path} (compact fallback)")
            else:
                print(f"Wrote {png_path} (via mermaid.ink fallback)")
            return
        except urllib.error.URLError:
            pass

    compact_path.write_text(compact_mermaid, encoding="utf-8")
    raise SystemExit(
        "Failed to render PNG from Mermaid for both full and compact diagrams. "
        f"Mermaid sources were generated at {mermaid_path} and {compact_path}."
    )


def build_fallback_plantuml(schema_path: Path) -> str:  # noqa: C901
    """Build a minimal PlantUML class diagram fallback from LinkML classes."""
    view = SchemaView(str(schema_path))
    lines: list[str] = [
        "@startuml",
        "hide methods",
        "hide stereotypes",
        "left to right direction",
        "skinparam linetype ortho",
    ]
    class_names = [str(name) for name in view.all_classes()]

    core_classes = {
        "Warehouse",
        "WarehouseManifest",
        "WarehouseCatalog",
        "CatalogView",
        "WarehouseTable",
        "WarehouseImage",
    }
    image_classes = {
        "ImageOMETiff",
        "ImageOMEZarr",
        "ImageTIFF",
        "TableImage",
        "TableSourceImages",
        "TableImageCrops",
        "TableImageLabel",
        "ImageReadContext",
    }
    analysis_classes = {
        "TableAnalyze",
        "TableDerivedMeasurement",
        "TableQualityControl",
        "TableEmbedding",
        "JoinKey",
        "ProvenanceRecord",
        "MixinExternalReference",
        "MixinProvenance",
    }

    def add_group(title: str, members: set[str]) -> None:
        present = [c for c in class_names if c in members]
        if not present:
            return
        lines.append(f'package "{title}" {{')
        for class_name in present:
            lines.append(f"class {class_name}")
        lines.append("}")

    add_group("Core Warehouse", core_classes)
    add_group("Image Domain", image_classes)
    add_group("Analysis Domain", analysis_classes)

    already = core_classes | image_classes | analysis_classes
    for class_name in class_names:
        if class_name not in already:
            lines.append(f"class {class_name}")

    for class_name in class_names:
        cls = view.get_class(class_name)
        if cls.is_a:
            lines.append(f"{class_name} --|> {cls.is_a!s}")
        for mixin in cls.mixins or []:
            lines.append(f"{class_name} ..|> {mixin!s}")

    # Keep visible structural "has-a" links tight and intentional.
    curated_links = [
        ("Warehouse", "WarehouseManifest", "manifest"),
        ("Warehouse", "WarehouseCatalog", "catalogs"),
        ("WarehouseCatalog", "CatalogView", "views"),
        ("CatalogView", "WarehouseTable", "source_tables"),
        ("CatalogView", "WarehouseImage", "view_images"),
        ("WarehouseManifest", "WarehouseTable", "tables"),
        ("WarehouseManifest", "WarehouseImage", "manifest_images"),
        ("WarehouseImage", "WarehouseManifest", "image_manifest"),
        ("WarehouseImage", "CatalogView", "image_views"),
        ("TableImageCrops", "TableSourceImages", "source_image_table"),
        ("TableSourceImages", "TableImageLabel", "label_table"),
        ("TableImageLabel", "TableSourceImages", "source_image_table"),
        ("TableQualityControl", "TableEmbedding", "related_embedding_tables"),
        (
            "TableQualityControl",
            "TableDerivedMeasurement",
            "related_measurement_tables",
        ),
    ]
    for src, dst, label in curated_links:
        if src in class_names and dst in class_names:
            lines.append(f'{src} --> {dst} : "{label}"')

    lines.extend(build_tier_layout_lines(set(class_names)))

    lines.append("@enduml")
    return "\n".join(lines) + "\n"


def build_presentation_plantuml(schema_path: Path) -> str:  # noqa: C901, PLR0912
    """Build a cleaner, presentation-focused PlantUML diagram."""
    view = SchemaView(str(schema_path))
    class_names = {str(name) for name in view.all_classes()}

    lines: list[str] = [
        "@startuml",
        "top to bottom direction",
        "skinparam linetype ortho",
        "skinparam classAttributeIconSize 0",
        "hide methods",
        "hide stereotypes",
        "",
        'package "Core Warehouse" {',
    ]
    for cls in [
        "Warehouse",
        "WarehouseManifest",
        "WarehouseCatalog",
        "Namespace",
        "CatalogView",
        "WarehouseTable",
        "WarehouseImage",
    ]:
        if cls in class_names:
            lines.append(f"class {cls}")
    lines.append("}")
    lines.append("")
    lines.append('package "Images Namespace" {')
    lines.append('package "Image Formats" {')
    for cls in ["ImageOMEZarr", "ImageOMETiff", "ImageTIFF"]:
        if cls in class_names:
            lines.append(f"class {cls}")
    lines.append("}")
    lines.append('package "Image Tables" {')
    for cls in [
        "TableImage",
        "TableSourceImages",
        "TableImageCrops",
        "TableImageLabel",
    ]:
        if cls in class_names:
            lines.append(f"class {cls}")
    lines.append("}")
    lines.append("}")
    lines.append("")
    lines.append('package "Analysis Tables" {')
    for cls in [
        "TableAnalyze",
        "TableDerivedMeasurement",
        "TableQualityControl",
        "TableEmbedding",
    ]:
        if cls in class_names:
            lines.append(f"class {cls}")
    lines.append("}")
    lines.append("")

    # Keep only high-signal relationships.
    rels = [
        ("Warehouse", "WarehouseManifest", "manifest"),
        ("Warehouse", "WarehouseCatalog", "catalogs"),
        ("WarehouseCatalog", "Namespace", "namespaces"),
        ("WarehouseCatalog", "CatalogView", "views"),
        ("CatalogView", "WarehouseTable", "source_tables"),
        ("WarehouseManifest", "WarehouseTable", "tables"),
        ("WarehouseManifest", "WarehouseImage", "manifest_images"),
        ("TableImage", "WarehouseTable", "is_a"),
        ("TableAnalyze", "WarehouseTable", "is_a"),
        ("TableSourceImages", "TableImage", "is_a"),
        ("TableImageCrops", "TableImage", "is_a"),
        ("TableImageLabel", "TableImage", "is_a"),
        ("TableDerivedMeasurement", "TableAnalyze", "is_a"),
        ("TableQualityControl", "TableAnalyze", "is_a"),
        ("TableEmbedding", "TableAnalyze", "is_a"),
        ("ImageOMEZarr", "WarehouseImage", "is_a"),
        ("ImageOMETiff", "WarehouseImage", "is_a"),
        ("ImageTIFF", "WarehouseImage", "is_a"),
        ("TableImageCrops", "TableSourceImages", "source_image_table"),
        ("TableQualityControl", "TableDerivedMeasurement", "optional"),
    ]
    for src, dst, label in rels:
        if src in class_names and dst in class_names:
            if label == "is_a":
                lines.append(f"{src} --|> {dst}")
            else:
                lines.append(f'{src} --> {dst} : "{label}"')

    # Lightweight layout anchors.
    anchors = [
        ("Warehouse", "WarehouseManifest"),
        ("Warehouse", "WarehouseCatalog"),
        ("WarehouseCatalog", "Namespace"),
        ("WarehouseCatalog", "CatalogView"),
        ("WarehouseManifest", "WarehouseTable"),
        ("WarehouseManifest", "WarehouseImage"),
    ]
    for src, dst in anchors:
        if src in class_names and dst in class_names:
            lines.append(f"{src} -[hidden]down-> {dst}")

    lines.append("@enduml")
    return "\n".join(lines) + "\n"


def build_tier_layout_lines(class_names: set[str]) -> list[str]:
    """Create minimal hidden constraints to keep PlantUML readable."""
    lines: list[str] = []
    # Keep core warehouse flow in one clean spine.
    if "Warehouse" in class_names and "WarehouseManifest" in class_names:
        lines.append("Warehouse -[hidden]right-> WarehouseManifest")
    if "WarehouseManifest" in class_names and "WarehouseCatalog" in class_names:
        lines.append("WarehouseManifest -[hidden]right-> WarehouseCatalog")
    if "WarehouseCatalog" in class_names and "CatalogView" in class_names:
        lines.append("WarehouseCatalog -[hidden]right-> CatalogView")
    if "CatalogView" in class_names and "WarehouseTable" in class_names:
        lines.append("CatalogView -[hidden]right-> WarehouseTable")
    if "WarehouseTable" in class_names and "WarehouseImage" in class_names:
        lines.append("WarehouseTable -[hidden]right-> WarehouseImage")

    # Keep image format subclasses adjacent.
    if {"ImageOMETiff", "ImageOMEZarr", "ImageTIFF"}.issubset(class_names):
        lines.append("ImageOMETiff -[hidden]right-> ImageOMEZarr")
        lines.append("ImageOMEZarr -[hidden]right-> ImageTIFF")

    return lines


def enforce_plantuml_tiers(plantuml_text: str) -> str:
    """Inject hidden tier links into generated PlantUML output."""
    marker = "@enduml"
    lines = plantuml_text.splitlines()
    class_names = set()
    for line in lines:
        if line.startswith("class "):
            class_names.add(line.removeprefix("class ").strip())
    tier_lines = build_tier_layout_lines(class_names)
    if not tier_lines:
        return plantuml_text

    # Keep this idempotent: remove existing tier constraints before re-adding.
    tier_set = set(tier_lines)
    filtered_lines = [line for line in lines if line.strip() not in tier_set]
    plantuml_text = "\n".join(filtered_lines) + "\n"

    block = "\n".join(tier_lines) + "\n"
    if marker in plantuml_text:
        return plantuml_text.replace(marker, f"{block}{marker}", 1)
    return plantuml_text + "\n" + block


if __name__ == "__main__":
    main()
