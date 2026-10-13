# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess

from dagster import get_dagster_logger
import geopandas as gpd
import pystac

"""
Builds a Portolan-formatted STAC catalog (https://github.com/portolan-sdi/portolan-spec)
describing the geoparquet and pmtiles files this pipeline has already exported to
the object store, and patches in the metadata the `portolan` CLI's external-asset
path cannot infer on its own (accurate bbox, providers, checksums, a PMTiles render
link, and a default thumbnail/style).

`portolan` is installed as a standalone tool in the user code image (see
Dockerfile_user_code), not as a project dependency: its pinned duckdb range
conflicts with geoparquet-io's, so it is invoked exclusively as a subprocess here,
the same way this codebase already shells out to qlever/oras/docker.
"""

GEOCONNEX_CATALOG_ID = "geoconnex"
GEOCONNEX_PROVIDER_NAME = "Internet of Water Coalition"
GEOCONNEX_CONTACT_URL = "cloftus@lincolninst.edu"
CATALOG_LICENSE = "CC0-1.0"  # must be an SPDX identifier for portolan
# markdown description of the whole catalog; edit the file to change it
CATALOG_DESCRIPTION_FILE = Path(__file__).parent / "stac_catalog_description.md"

PMTILES_MEDIA_TYPE = "application/vnd.pmtiles"
MAPLIBRE_STYLE_MEDIA_TYPE = "application/vnd.mapbox.style+json"

TABLE_EXTENSION_SCHEMA = "https://stac-extensions.github.io/table/v1.2.0/schema.json"
FILE_EXTENSION_SCHEMA = "https://stac-extensions.github.io/file/v2.1.0/schema.json"
WEB_MAP_LINKS_EXTENSION_SCHEMA = (
    "https://stac-extensions.github.io/web-map-links/v1.3.0/schema.json"
)

# the subset of columns exported in the per-sitemap geoparquet files;
# kept in sync with GEOPARQUET_COLUMNS in assetGroups/export.py
GEOPARQUET_TABLE_COLUMNS = [
    {"name": "id", "type": "string"},
    {"name": "geoconnex_sitemap", "type": "string"},
    {"name": "feature_name", "type": "string"},
    {"name": "feature_description", "type": "string"},
    {"name": "mainstem_uri", "type": "string"},
    {"name": "geometry", "type": "binary", "description": "WKB-encoded geometry"},
]


def multihash_sha256(data: bytes) -> str:
    """Multihash-encode a sha256 digest, as required for STAC file:checksum"""
    return f"1220{hashlib.sha256(data).hexdigest()}"


def humanize_sitemap_id(sitemap_id: str) -> str:
    """Turn a sitemap id like 'monitoring_locations' into a human-readable title"""
    return sitemap_id.replace("_", " ").replace("-", " ").title()


def stac_collection_id(sitemap_id: str) -> str:
    """
    Turn a sitemap id like 'ca_gage_assessment:gages' into a valid Portolan
    collection id ('ca_gage_assessment-gages'). Portolan only accepts lowercase
    letters, numbers, hyphens and underscores (slashes would nest collections),
    starting with a letter or number, and offers no flag to skip that check.
    This id only names the collection's directory; `patch_sitemap_collection`
    restores the raw sitemap id as the STAC `id` afterwards.
    """
    collection_id = re.sub(r"[^a-z0-9_-]+", "-", sitemap_id.lower())
    collection_id = re.sub(r"-+", "-", collection_id).strip("-_")
    if not collection_id:
        raise ValueError(
            f"Sitemap id '{sitemap_id}' has no characters usable in a collection id"
        )
    return collection_id


def geoconnex_providers() -> list[dict]:
    """
    The STAC providers array for a collection that Geoconnex both produces and
    hosts itself: a producer and, listed last, a host with contact info.
    """
    return [
        {"name": GEOCONNEX_PROVIDER_NAME, "roles": ["producer"]},
        {
            "name": GEOCONNEX_PROVIDER_NAME,
            "roles": ["host"],
            "url": GEOCONNEX_CONTACT_URL,
        },
    ]


def maplibre_style_for_pmtiles(pmtiles_url: str, layer: str) -> dict:
    """A minimal MapLibre GL style (spec v8) rendering a PMTiles layer with default styling"""
    return {
        "version": 8,
        "name": layer,
        "sources": {
            "data": {
                "type": "vector",
                "url": f"pmtiles://{pmtiles_url}",
            }
        },
        "layers": [
            {
                "id": f"{layer}-points",
                "type": "circle",
                "source": "data",
                "source-layer": layer,
                "paint": {
                    "circle-radius": 3,
                    "circle-color": "#2563eb",
                    "circle-stroke-width": 1,
                    "circle-stroke-color": "#ffffff",
                },
            }
        ],
    }


def render_thumbnail(gdf: gpd.GeoDataFrame, output_path: Path) -> None:
    """Render a static default-styled preview image of a collection's features"""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(4, 4), dpi=100)
    gdf.plot(ax=ax, markersize=4, color="#2563eb")
    ax.set_axis_off()
    fig.tight_layout(pad=0)
    fig.savefig(output_path, facecolor="white")
    plt.close(fig)


def _run_portolan(args: list[str], cwd: Path) -> dict:
    """
    Run the `portolan` CLI with machine-readable output and return its parsed
    result. Raises if the command itself fails to run (a non-conformant
    catalog is the caller's problem to decide on, not this helper's).
    """
    command = ["portolan", *args, "--json"]
    get_dagster_logger().info(
        f"Running '{' '.join(shlex.quote(c) for c in command)}' in {cwd}"
    )
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
    if result.stderr:
        get_dagster_logger().warning(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"'{' '.join(command)}' failed with exit code {result.returncode}: "
            f"{result.stdout}\n{result.stderr}"
        )
    return json.loads(result.stdout) if result.stdout else {}


def init_catalog(catalog_root: Path) -> None:
    """Initialize a new, empty Portolan catalog at `catalog_root`"""
    _run_portolan(
        [
            "init",
            "--auto",
            "--id",
            GEOCONNEX_CATALOG_ID,
            "--title",
            "Geoconnex Spatial Data Catalog",
            "--description",
            CATALOG_DESCRIPTION_FILE.read_text().strip(),
            "--license",
            CATALOG_LICENSE,
        ],
        cwd=catalog_root,
    )


def register_sitemap_collection(
    catalog_root: Path, *, sitemap_id: str, parquet_url: str, bbox: list[float]
) -> Path:
    """
    Register one sitemap's geoparquet export as an external Portolan collection,
    referenced in place rather than copied. Returns the path to its collection.json.
    """
    title = humanize_sitemap_id(sitemap_id)
    description = (
        f"Geoconnex features harvested from the '{sitemap_id}' sitemap, published "
        f"as GeoParquet by the {GEOCONNEX_PROVIDER_NAME}. Source: automated harvest "
        f"of the '{sitemap_id}' sitemap by the Geoconnex scheduler, licensed "
        f"{CATALOG_LICENSE}."
    )
    result = _run_portolan(
        [
            "add-external",
            parquet_url,
            "--collection",
            stac_collection_id(sitemap_id),
            "--title",
            title,
            "--description",
            description,
            "--license",
            CATALOG_LICENSE,
            "--bbox",
            ",".join(str(round(v, 6)) for v in bbox),
        ],
        cwd=catalog_root,
    )
    return Path(result["data"]["collection_path"])


def patch_sitemap_collection(
    collection_path: Path,
    *,
    sitemap_id: str,
    parquet_size: int,
    parquet_checksum: str,
    pmtiles_url: str | None = None,
    pmtiles_layer: str | None = None,
    thumbnail_filename: str | None = None,
    style_filename: str | None = None,
) -> None:
    """
    Patch a collection.json that `register_sitemap_collection` already created with
    the metadata the CLI's external-asset path cannot infer on its own: the raw
    sitemap id as the collection id, correct
    providers (it is not a mirror, Geoconnex hosts the data it produces), the
    GeoParquet table schema and checksum, an optional PMTiles render link, and
    optional local thumbnail/style assets (expected to already sit alongside
    collection.json, named `thumbnail_filename`/`style_filename`).
    """
    collection = pystac.Collection.from_file(str(collection_path))

    # the collection directory has to use the sanitized `stac_collection_id`,
    # since characters like ':' break relative hrefs and portolan rejects them,
    # but the STAC id itself should match the sitemap it describes. portolan
    # check reports this as a PTL-COL-003 naming warning, not an error.
    collection.id = sitemap_id

    # add-external always emits a rel:via link, assuming the data is a
    # third-party mirror; this catalog's producer and host are the same
    # organization, so keeping it would misclassify an official collection
    # as a mirror of someone else's data.
    collection.links = [link for link in collection.links if link.rel != "via"]
    collection.providers = [pystac.Provider(**p) for p in geoconnex_providers()]

    extensions = set(collection.stac_extensions or [])
    extensions.add(TABLE_EXTENSION_SCHEMA)
    extensions.add(FILE_EXTENSION_SCHEMA)

    data_asset = collection.assets["data"]
    data_asset.extra_fields["table:columns"] = GEOPARQUET_TABLE_COLUMNS
    data_asset.extra_fields["file:size"] = parquet_size
    data_asset.extra_fields["file:checksum"] = parquet_checksum

    if pmtiles_url and pmtiles_layer:
        collection.add_link(
            pystac.Link(
                rel="pmtiles",
                target=pmtiles_url,
                media_type=PMTILES_MEDIA_TYPE,
                extra_fields={"pmtiles:layers": [pmtiles_layer]},
            )
        )
        extensions.add(WEB_MAP_LINKS_EXTENSION_SCHEMA)

    if thumbnail_filename:
        collection.add_asset(
            "thumbnail",
            pystac.Asset(
                href=f"./{thumbnail_filename}",
                media_type="image/png",
                title="Default thumbnail",
                roles=["thumbnail"],
            ),
        )

    if style_filename:
        collection.add_asset(
            "style_default",
            pystac.Asset(
                href=f"./{style_filename}",
                media_type=MAPLIBRE_STYLE_MEDIA_TYPE,
                title="Default MapLibre style",
                roles=["style", "default"],
            ),
        )

    collection.stac_extensions = sorted(extensions)
    collection.save_object(include_self_link=False)


def finalize_catalog(catalog_root: Path) -> None:
    """
    Run Portolan's own mechanical fixers (schema URIs, README-link backfill),
    regenerate READMEs from the finished metadata, then validate. Raises if the
    catalog still fails to conform once Portolan's fixers have run.
    """
    # --no-data: the parquet assets are remote files in s3 which Portolan's data
    # fixers can't rewrite, so a data pass here can only fail; the parquet is
    # instead written to spec in optimize_harvested_parquet_file
    _run_portolan(["check", "--fix", "--no-data"], cwd=catalog_root)
    _run_portolan(["readme"], cwd=catalog_root)

    command = ["portolan", "check", "--no-data", "--json"]
    result = subprocess.run(command, cwd=catalog_root, capture_output=True, text=True)
    report = json.loads(result.stdout) if result.stdout else {}
    data = report.get("data", {})
    error_count = data.get("error_count")
    if error_count is None:
        # unexpected output shape; fail closed rather than silently publish
        # a catalog we never actually validated
        raise RuntimeError(
            f"Could not parse 'portolan check' output: {result.stdout}\n{result.stderr}"
        )
    if data.get("warning_count"):
        get_dagster_logger().warning(
            f"Generated Portolan STAC catalog has {data['warning_count']} warning(s): "
            f"{json.dumps(data.get('findings'))}"
        )
    if error_count:
        raise RuntimeError(
            f"Generated Portolan STAC catalog failed validation with {error_count} "
            f"error(s): {json.dumps(data.get('findings'))}"
        )
