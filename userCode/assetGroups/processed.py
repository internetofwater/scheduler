# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import io
import json
from pathlib import Path
import tempfile

from dagster import asset, get_dagster_logger
import geopandas as gpd
import pandas as pd
import pyogrio
import requests

from userCode.lib.classes import S3
from userCode.lib.env import PROCESSED_PREFIX_IN_S3, S3_ADDRESS
from userCode.lib.stac_catalog import multihash_sha256_file, render_thumbnail

"""
All assets in this asset group derive datasets from upstream data products
and publish them under processed/ in the object store, where the rest of the
pipeline (and the public STAC catalog) picks them up. They are expensive to
generate and their inputs rarely change, so they are materialized by hand
rather than on every harvest.
"""

PROCESSED_GROUP = "processed"

# the USGS Reference Hydrologic Geospatial Fabric, whose NHDPlusV2 reference
# catchments are joined to mainstems
REFERENCE_HYDROFABRIC_URL = (
    "https://www.sciencebase.gov/catalog/item/61295190d34e40dd9c06bcd7"
)
# ScienceBase only serves reference_catchments.gpkg after a login, so an
# unmodified copy of it is kept in the default bucket alongside the outputs
REFERENCE_CATCHMENTS_INPUT_PATH_IN_S3 = (
    f"{PROCESSED_PREFIX_IN_S3}intermediate_inputs/reference_catchments.gpkg"
)
# The mainstems are joined with the NHDPlusV2 COMID lookup from v3.2, the
# current mainstem release for every table except the NHDPlusHR lookup (which
# v3.3 re-releases). The reference catchments are only NHDPlusV2 catchments,
# and most v3 mainstems are on NHDPlusV2 flowlines; the few v3 mainstems that
# are only on NHDPlusHR will not get associations. Per the ref_rivers
# maintainer, bringing in the NHDPlusHR crosswalk is premature while the v3
# hydrographic framework is still being backfilled, so do not add it yet.
REF_RIVERS_RELEASE = "v3.2"
REF_RIVERS_RELEASE_URL = (
    f"https://github.com/internetofwater/ref_rivers/releases/tag/{REF_RIVERS_RELEASE}"
)
NHDPV2_MAINSTEM_LOOKUP_URL = (
    "https://github.com/internetofwater/ref_rivers/releases/download/"
    f"{REF_RIVERS_RELEASE}/nhdpv2_lookup.csv"
)
MAINSTEMS_URL = (
    "https://github.com/internetofwater/ref_rivers/releases/download/"
    f"{REF_RIVERS_RELEASE}/mainstems.gpkg"
)
SOURCE_CODE_URL = "https://github.com/internetofwater/scheduler/blob/main/userCode/assetGroups/processed.py"

# the name is kept from the previous version of this file, which nabu and the
# MAINSTEM_FILE env var already refer to
REFERENCE_CATCHMENTS_NAME = "reference_catchments_and_flowlines"
REFERENCE_CATCHMENTS_PATH_IN_S3 = (
    f"{PROCESSED_PREFIX_IN_S3}{REFERENCE_CATCHMENTS_NAME}.fgb"
)
# a summary of the flatgeobuf (bbox, size, checksum, sources) and a preview of
# it, so the STAC catalog can describe it without downloading the whole file
REFERENCE_CATCHMENTS_METADATA_PATH_IN_S3 = (
    f"{PROCESSED_PREFIX_IN_S3}{REFERENCE_CATCHMENTS_NAME}.json"
)
REFERENCE_CATCHMENTS_THUMBNAIL_PATH_IN_S3 = (
    f"{PROCESSED_PREFIX_IN_S3}{REFERENCE_CATCHMENTS_NAME}.png"
)
FLATGEOBUF_MEDIA_TYPE = "application/vnd.flatgeobuf"

# the reference catchment attributes kept in the output, prefixed with
# 'Catchment_' to match the previous version of this file
CATCHMENT_COLUMNS = ["featureid", "areasqkm", "vpuid", "rpuid"]
# the column nabu reads the mainstem uri from
MAINSTEM_URI_COLUMN = "geoconnex_url"

REFERENCE_CATCHMENTS_TABLE_COLUMNS = [
    {
        "name": "Catchment_featureid",
        "type": "int64",
        "description": "NHDPlusV2 COMID of the catchment and its flowline",
    },
    {
        "name": "Catchment_areasqkm",
        "type": "float64",
        "description": "Catchment area in square kilometers",
    },
    {
        "name": "Catchment_vpuid",
        "type": "string",
        "description": "NHDPlusV2 vector processing unit",
    },
    {
        "name": "Catchment_rpuid",
        "type": "string",
        "description": "NHDPlusV2 raster processing unit",
    },
    {
        "name": MAINSTEM_URI_COLUMN,
        "type": "string",
        "description": "Geoconnex reference mainstem uri of the catchment's flowline; "
        "empty if the flowline is not part of a mainstem",
    },
    {"name": "geometry", "type": "geometry", "description": "Polygon in EPSG:4326"},
]

REFERENCE_CATCHMENTS_TITLE = "Reference Catchments with Geoconnex Mainstems"
REFERENCE_CATCHMENTS_DESCRIPTION = (
    "Every NHDPlusV2 catchment in the USGS Reference Hydrologic Geospatial Fabric "
    f"({REFERENCE_HYDROFABRIC_URL}) joined to the uri of the Geoconnex reference "
    "mainstem its flowline is part of, derived from the NHDPlusV2 lookup in the "
    f"ref_rivers {REF_RIVERS_RELEASE} release ({REF_RIVERS_RELEASE_URL}).\n\n"
    "Geoconnex uses this file during the harvest to associate each feature with "
    "the mainstem of the catchment containing it. The mainstem uri is in the "
    f"'{MAINSTEM_URI_COLUMN}' column, which is empty for catchments whose flowline "
    "is not part of a mainstem.\n\n"
    f"The source code that generated this file can be found at {SOURCE_CODE_URL}"
)


def download_file(url: str, path: Path) -> None:
    """Stream a remote file to `path` without loading it into memory"""
    get_dagster_logger().info(f"Downloading {url} to {path}")
    FIFTEEN_MINUTES = 60 * 15
    with requests.get(url, stream=True, timeout=FIFTEEN_MINUTES) as response:
        response.raise_for_status()
        with path.open("wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)


def join_catchments_to_mainstems(
    catchments: gpd.GeoDataFrame,
    mainstem_lookup: pd.DataFrame,
    mainstems: pd.DataFrame,
) -> gpd.GeoDataFrame:
    """
    Join each reference catchment to the uri of the mainstem its flowline is
    part of, so that the catchment containing a feature gives its mainstem.
    `catchments` are the NHDPlusV2 reference catchments, keyed by `featureid`
    (the comid of their flowline), `mainstem_lookup` maps each `comid` to a
    mainstem `uri`, and `mainstems` is the `uri` and `superseded` flag of every
    mainstem in the same ref_rivers release, used to check that the lookup
    only refers to current mainstems.
    """
    mainstem_lookup = mainstem_lookup.astype({"comid": "Int64", "uri": "string"})
    duplicate_comids = mainstem_lookup["comid"].duplicated()
    if duplicate_comids.any():
        raise ValueError(
            f"{duplicate_comids.sum()} comids map to more than one mainstem in the "
            "mainstem lookup, so their catchments would be duplicated"
        )

    lookup_uris = set(mainstem_lookup["uri"].dropna())
    unknown_uris = lookup_uris - set(mainstems["uri"])
    if unknown_uris:
        raise ValueError(
            f"{len(unknown_uris)} mainstems in the lookup are not in the mainstems "
            f"release, e.g. {sorted(unknown_uris)[:5]}"
        )
    superseded_uris = lookup_uris & set(
        mainstems.loc[mainstems["superseded"].fillna(False).astype(bool), "uri"]
    )
    if superseded_uris:
        raise ValueError(
            f"{len(superseded_uris)} mainstems in the lookup have been superseded, "
            f"e.g. {sorted(superseded_uris)[:5]}"
        )

    featureids = pd.Series(catchments["featureid"])
    if featureids.isna().any() or not featureids.mod(1).eq(0).all():
        raise ValueError("Every catchment featureid must be an integer comid")

    selected = gpd.GeoDataFrame(
        catchments[[*CATCHMENT_COLUMNS, catchments.geometry.name]]
    )
    selected["featureid"] = featureids.astype("Int64")
    selected = selected.to_crs("EPSG:4326")
    # the geometry column is named 'geom' in some sources and 'geometry' in others
    if selected.geometry.name != "geometry":
        selected = selected.rename_geometry("geometry")

    joined = selected.merge(
        mainstem_lookup,
        left_on="featureid",
        right_on="comid",
        how="left",
        validate="many_to_one",
    ).drop(columns="comid")
    joined = joined.rename(
        columns={
            **{column: f"Catchment_{column}" for column in CATCHMENT_COLUMNS},
            "uri": MAINSTEM_URI_COLUMN,
        }
    )
    assert len(joined) == len(selected), "The join should not add or drop catchments"
    # in the order the STAC catalog describes them
    columns = [column["name"] for column in REFERENCE_CATCHMENTS_TABLE_COLUMNS]
    return gpd.GeoDataFrame(joined[columns], geometry="geometry", crs="EPSG:4326")


@asset(group_name=PROCESSED_GROUP)
def reference_catchments_and_mainstems():
    """
    Generate the flatgeobuf of reference catchments joined to the mainstem
    of their flowline, which nabu uses to associate harvested features with
    mainstems, and publish it to processed/ in the object store along with a
    metadata summary and thumbnail for the STAC catalog
    """
    logger = get_dagster_logger()
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        catchments_file = tmp_dir / "reference_catchments.gpkg"
        lookup_file = tmp_dir / "nhdpv2_lookup.csv"
        mainstems_file = tmp_dir / "mainstems.gpkg"
        s3 = S3()
        logger.info(f"Downloading {REFERENCE_CATCHMENTS_INPUT_PATH_IN_S3}")
        s3.client.fget_object(
            s3.bucket, REFERENCE_CATCHMENTS_INPUT_PATH_IN_S3, str(catchments_file)
        )
        download_file(NHDPV2_MAINSTEM_LOOKUP_URL, lookup_file)
        download_file(MAINSTEMS_URL, mainstems_file)

        joined = join_catchments_to_mainstems(
            gpd.read_file(catchments_file, engine="pyogrio", use_arrow=True),
            pd.read_csv(lookup_file),
            pyogrio.read_dataframe(
                mainstems_file, columns=["uri", "superseded"], read_geometry=False
            ),
        )
        with_mainstem = int(joined[MAINSTEM_URI_COLUMN].notna().sum())
        logger.info(
            f"{with_mainstem} of {len(joined)} catchments have a mainstem "
            f"({100 * (1 - with_mainstem / len(joined)):.2f}% do not)"
        )

        fgb_file = tmp_dir / f"{REFERENCE_CATCHMENTS_NAME}.fgb"
        logger.info(f"Writing {fgb_file}")
        # pyogrio writes a spatial index by default, which nabu relies on
        # for fast lookups of the catchment containing each feature
        joined.to_file(
            fgb_file,
            driver="FlatGeobuf",
            engine="pyogrio",
            layer=REFERENCE_CATCHMENTS_NAME,
        )

        thumbnail_file = tmp_dir / f"{REFERENCE_CATCHMENTS_NAME}.png"
        render_thumbnail(joined, thumbnail_file)

        metadata = {
            "bbox": [round(float(v), 6) for v in joined.total_bounds],
            "feature_count": len(joined),
            "features_with_mainstem": with_mainstem,
            "file:size": fgb_file.stat().st_size,
            "file:checksum": multihash_sha256_file(fgb_file),
            "sources": [
                {
                    "href": REFERENCE_HYDROFABRIC_URL,
                    "title": "USGS Reference Hydrofabric",
                },
                {
                    "href": f"https://{S3_ADDRESS}/{s3.bucket}/"
                    f"{REFERENCE_CATCHMENTS_INPUT_PATH_IN_S3}",
                    "title": "USGS Reference Hydrofabric reference catchments",
                },
                {
                    "href": NHDPV2_MAINSTEM_LOOKUP_URL,
                    "title": f"ref_rivers {REF_RIVERS_RELEASE} NHDPlusV2 mainstem lookup",
                },
                {
                    "href": MAINSTEMS_URL,
                    "title": f"ref_rivers {REF_RIVERS_RELEASE} mainstems",
                },
            ],
        }

        # the metadata is uploaded last so that it only describes a
        # flatgeobuf which has finished uploading
        for file, remote_path, content_type in (
            (fgb_file, REFERENCE_CATCHMENTS_PATH_IN_S3, FLATGEOBUF_MEDIA_TYPE),
            (thumbnail_file, REFERENCE_CATCHMENTS_THUMBNAIL_PATH_IN_S3, "image/png"),
        ):
            logger.info(f"Uploading {remote_path} to bucket '{s3.bucket}'")
            with file.open("rb") as f:
                s3.load_stream(
                    stream=f,
                    remote_path=remote_path,
                    content_length=file.stat().st_size,
                    content_type=content_type,
                    headers={},
                )
        metadata_bytes = json.dumps(metadata, indent=2).encode()
        s3.client.put_object(
            s3.bucket,
            REFERENCE_CATCHMENTS_METADATA_PATH_IN_S3,
            io.BytesIO(metadata_bytes),
            len(metadata_bytes),
            content_type="application/json",
        )
