# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess
from xml.etree import ElementTree as ET

from dagster import get_dagster_logger
import geopandas as gpd
import numpy as np
import pystac
import shapely

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


def multihash_sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    """
    Same as `multihash_sha256` for the contents of a file, read in chunks so
    multi-gigabyte exports are never loaded into memory at once
    """
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            digest.update(chunk)
    return f"1220{digest.hexdigest()}"


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

    def geometry_type_is(*types: str) -> list:
        # match both single and multi variants since newer MapLibre versions
        # report e.g. MultiPolygon distinctly from Polygon
        return ["match", ["geometry-type"], list(types), True, False]

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
                "id": f"{layer}-polygons",
                "type": "fill",
                "source": "data",
                "source-layer": layer,
                "filter": geometry_type_is("Polygon", "MultiPolygon"),
                "paint": {
                    "fill-color": "#2563eb",
                    "fill-opacity": 0.3,
                    "fill-outline-color": "#1e40af",
                },
            },
            {
                "id": f"{layer}-lines",
                "type": "line",
                "source": "data",
                "source-layer": layer,
                "filter": geometry_type_is("LineString", "MultiLineString"),
                "paint": {
                    "line-color": "#2563eb",
                    "line-width": 1.5,
                },
            },
            {
                "id": f"{layer}-points",
                "type": "circle",
                "source": "data",
                "source-layer": layer,
                "filter": geometry_type_is("Point", "MultiPoint"),
                "paint": {
                    "circle-radius": 3,
                    "circle-color": "#2563eb",
                    "circle-stroke-width": 1,
                    "circle-stroke-color": "#ffffff",
                },
            },
        ],
    }


# the Portolan browser shows thumbnails in cards about 350-700px wide and
# 250px tall, so they are rendered at 3:2 to fill the card without cropping
THUMBNAIL_WIDTH_PX = 600
THUMBNAIL_HEIGHT_PX = 400
# plotting every feature of a large sitemap (e.g. detailed polygons) takes far
# longer than the rest of catalog generation, and a thumbnail cannot show
# more than this many features anyway
THUMBNAIL_MAX_FEATURES = 20_000


def thumbnail_features(
    gdf: gpd.GeoDataFrame, max_features: int = THUMBNAIL_MAX_FEATURES
) -> gpd.GeoDataFrame:
    """
    The features to draw in a thumbnail: a random sample of at most
    `max_features`, so that plotting cost is bounded regardless of the size
    of the sitemap
    """
    if len(gdf) > max_features:
        # a fixed seed keeps the thumbnail, and so its checksum, stable across runs
        gdf = gdf.sample(n=max_features, random_state=0)
    return gdf


# the share of the data, by size, that a thumbnail may leave out of its frame
# on each side, so a few outliers (e.g. a territory across the antimeridian, or
# a misplaced point) do not zoom the thumbnail out to the whole world
THUMBNAIL_OUTLIER_FRACTION = 0.01


def _weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    order = np.argsort(values)
    cumulative = np.cumsum(weights[order])
    index = np.searchsorted(cumulative, q * cumulative[-1])
    return float(values[order][min(index, len(values) - 1)])


def thumbnail_extent(
    gdf: gpd.GeoDataFrame, outlier_fraction: float = THUMBNAIL_OUTLIER_FRACTION
) -> tuple[float, float, float, float]:
    """
    The EPSG:4326 extent a thumbnail of these features should show: the bounds
    of all but `outlier_fraction` of the data on each side. Multi-part features
    are split into their parts and each part is weighted by its area, length,
    or as one point, so that small far-off parts like the western Aleutians or
    Guam are left out while large features like Alaska stay whole
    """
    parts = shapely.get_parts(gdf.geometry.values)
    weights = shapely.area(parts)
    if not weights.any():
        weights = shapely.length(parts)
    if not weights.any():
        weights = np.ones(len(parts))
    bounds = shapely.bounds(parts)
    low, high = outlier_fraction, 1 - outlier_fraction
    return (
        _weighted_quantile(bounds[:, 0], weights, low),
        _weighted_quantile(bounds[:, 1], weights, low),
        _weighted_quantile(bounds[:, 2], weights, high),
        _weighted_quantile(bounds[:, 3], weights, high),
    )


# web mercator, which basemap tiles are served in, is undefined at the poles
WEB_MERCATOR_MAX_LATITUDE = 85.0511
# meters per degree of longitude in web mercator, at every latitude
WEB_MERCATOR_METERS_PER_DEGREE = 111_319.49
# Esri's light gray canvas, a plain basemap made to sit under data that needs no
# api key (Carto's basemaps now watermark every tile requested without one)
THUMBNAIL_BASEMAP_URL = (
    "https://server.arcgisonline.com/ArcGIS/rest/services/"
    "Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}"
)
THUMBNAIL_BASEMAP_ATTRIBUTION = "Tiles (C) Esri -- Esri, DeLorme, NAVTEQ"
# a thumbnail of a single point (or a tiny extent) still shows this much map
# around it, in web mercator meters, so the basemap gives it context
THUMBNAIL_MIN_SPAN_METERS = 20_000


def thumbnail_frame(
    bounds: tuple[float, float, float, float],
) -> tuple[float, float, float, float]:
    """
    The 3:2 web mercator frame for a thumbnail of features within the given
    EPSG:4326 bounds: their extent with a small margin, widened along one axis
    to the thumbnail's aspect ratio so the basemap fills the whole image
    instead of being letterboxed
    """
    min_x, min_y, max_x, max_y = map(
        float,
        gpd.GeoSeries.from_xy(
            [bounds[0], bounds[2]],
            [
                max(bounds[1], -WEB_MERCATOR_MAX_LATITUDE),
                min(bounds[3], WEB_MERCATOR_MAX_LATITUDE),
            ],
            crs="EPSG:4326",
        )
        .to_crs("EPSG:3857")
        .total_bounds,
    )
    aspect = THUMBNAIL_WIDTH_PX / THUMBNAIL_HEIGHT_PX
    width = max(max_x - min_x, THUMBNAIL_MIN_SPAN_METERS) * 1.05
    height = max(max_y - min_y, THUMBNAIL_MIN_SPAN_METERS) * 1.05
    width, height = max(width, height * aspect), max(height, width / aspect)
    center_x, center_y = (min_x + max_x) / 2, (min_y + max_y) / 2
    return (
        center_x - width / 2,
        center_y - height / 2,
        center_x + width / 2,
        center_y + height / 2,
    )


def render_thumbnail(
    gdf: gpd.GeoDataFrame, output_path: Path, basemap: bool = True
) -> None:
    """
    Render a static default-styled preview image of a collection's features
    over a basemap. The basemap tiles are fetched over the network, so if they
    cannot be fetched the features are rendered on a plain background instead
    of failing the catalog build
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    features = thumbnail_features(gdf)
    frame_min_x, frame_min_y, frame_max_x, frame_max_y = thumbnail_frame(
        thumbnail_extent(features)
    )

    # simplify to about one pixel of the frame; polygons smaller than that
    # collapse to empty geometries, so keep their originals to still show a dot
    pixel_degrees = (
        (frame_max_x - frame_min_x) / THUMBNAIL_WIDTH_PX
    ) / WEB_MERCATOR_METERS_PER_DEGREE
    simplified = features.geometry.simplify(pixel_degrees, preserve_topology=False)
    geometry = simplified.where(~simplified.is_empty, features.geometry)
    geometry = geometry.clip_by_rect(
        -180, -WEB_MERCATOR_MAX_LATITUDE, 180, WEB_MERCATOR_MAX_LATITUDE
    )
    geometry = geometry.loc[~geometry.is_empty].to_crs("EPSG:3857")

    fig = plt.figure(
        figsize=(THUMBNAIL_WIDTH_PX / 100, THUMBNAIL_HEIGHT_PX / 100), dpi=100
    )
    # the frame has the figure's aspect ratio, so the map fills it edge to edge
    ax = fig.add_axes((0, 0, 1, 1))
    geom_types = geometry.geom_type
    polygons = geometry.loc[geom_types.isin(["Polygon", "MultiPolygon"])]
    lines = geometry.loc[geom_types.isin(["LineString", "MultiLineString"])]
    points = geometry.loc[geom_types.isin(["Point", "MultiPoint"])]
    # translucent fills with solid outlines so dense polygons such as small
    # watersheds read as a mesh over the basemap rather than a solid mass
    if not polygons.empty:
        polygons.plot(
            ax=ax,
            facecolor=(0.145, 0.388, 0.922, 0.35),
            edgecolor="#1e40af",
            linewidth=0.4,
        )
    if not lines.empty:
        lines.plot(ax=ax, color="#2563eb", linewidth=0.8)
    if not points.empty:
        points.plot(
            ax=ax,
            markersize=4,
            color="#2563eb",
            edgecolor="#1e40af",
            linewidth=0.3,
        )
    # set before adding the basemap, which fetches tiles for the current limits
    ax.set_xlim(frame_min_x, frame_max_x)
    ax.set_ylim(frame_min_y, frame_max_y)
    ax.set_axis_off()

    if basemap:
        import contextily

        try:
            contextily.add_basemap(
                ax,
                crs="EPSG:3857",
                source=THUMBNAIL_BASEMAP_URL,
                attribution=THUMBNAIL_BASEMAP_ATTRIBUTION,
                attribution_size=5,
            )
        except Exception as e:
            get_dagster_logger().warning(
                f"Could not add a basemap to {output_path}, rendering it on a "
                f"plain background instead: {e}"
            )

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


SITEMAP_INDEX_NAMESPACES = {
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
    "geoconnex": "https://geoconnex.us",
}


def sitemap_dataset_descriptions(sitemap_index_xml: str) -> dict[str, str]:
    """
    Map each geoconnex:sitemap_id in a sitemap index to its
    geoconnex:dataset_description, skipping sitemaps without one
    """
    root = ET.fromstring(sitemap_index_xml)
    descriptions: dict[str, str] = {}
    for sitemap in root.findall("sm:sitemap", namespaces=SITEMAP_INDEX_NAMESPACES):
        sitemap_id = sitemap.findtext(
            "geoconnex:sitemap_id", namespaces=SITEMAP_INDEX_NAMESPACES
        )
        description = sitemap.findtext(
            "geoconnex:dataset_description", namespaces=SITEMAP_INDEX_NAMESPACES
        )
        if sitemap_id and sitemap_id.strip() and description and description.strip():
            descriptions[sitemap_id.strip()] = description.strip()
    return descriptions


def register_sitemap_collection(
    catalog_root: Path,
    *,
    sitemap_id: str,
    parquet_url: str,
    bbox: list[float],
    dataset_description: str | None = None,
) -> Path:
    """
    Register one sitemap's geoparquet export as an external Portolan collection,
    referenced in place rather than copied. Returns the path to its collection.json.
    `dataset_description` is the description the sitemap index gives for the
    dataset, and leads the collection description when present.
    """
    title = humanize_sitemap_id(sitemap_id)
    description = (
        f"Geoconnex features harvested from the '{sitemap_id}' sitemap, published "
        f"as GeoParquet by the {GEOCONNEX_PROVIDER_NAME}. Source: automated harvest "
        f"of the '{sitemap_id}' sitemap by the Geoconnex scheduler, licensed "
        f"{CATALOG_LICENSE}."
    )
    if dataset_description:
        description = f"{dataset_description}\n\n{description}"
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
        # map viewers like the Portolan browser only render tiles from assets,
        # not links; with this present they draw the PMTiles instead of
        # downloading the entire GeoParquet file, and the default style
        # applies to it since its source url matches this href
        collection.add_asset(
            "pmtiles",
            pystac.Asset(
                href=pmtiles_url,
                media_type=PMTILES_MEDIA_TYPE,
                title="Vector tiles",
                description="Features as PMTiles vector tiles for web visualization.",
                roles=["visual"],
            ),
        )

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
