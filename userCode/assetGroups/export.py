# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime
import io
import json
import math
import mimetypes
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from typing import Literal

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AutomationCondition,
    Config,
    asset,
    asset_check,
    get_dagster_logger,
)
import docker
from docker.errors import NotFound
import geopandas as gpd
import geoparquet_io as gpio
from geoparquet_io.core.partition.common import sanitize_filename
from minio.error import S3Error
import numpy as np
import pandas as pd
from pandas.io.sql import SQLTable
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import shapely
from sqlalchemy import Connection, text

from userCode.assetGroups.harvest import harvest_bulk_sitemap, harvest_sitemap
from userCode.assetGroups.processed import (
    REFERENCE_CATCHMENTS_DESCRIPTION,
    REFERENCE_CATCHMENTS_METADATA_PATH_IN_S3,
    REFERENCE_CATCHMENTS_NAME,
    REFERENCE_CATCHMENTS_PATH_IN_S3,
    REFERENCE_CATCHMENTS_TABLE_COLUMNS,
    REFERENCE_CATCHMENTS_THUMBNAIL_PATH_IN_S3,
    REFERENCE_CATCHMENTS_TITLE,
)
from userCode.lib.classes import RcloneClient, S3
from userCode.lib.containers import (
    NquadsConfig,
    NquadsContainer,
)
from userCode.lib.dagster import (
    all_dependencies_materialized,
)
from userCode.lib.env import (
    ASSETS_DIRECTORY,
    EXPORTS_PREFIX_IN_S3,
    GEOCONNEX_GRAPH_DIRECTORY,
    GEOCONNEX_INDEX_DIRECTORY,
    GHCR_TOKEN,
    HARVESTED_PARQUET_DIRECTORY,
    RDF_PARQUET_PREFIX_IN_S3,
    STRUCTURED_PARQUET_PREFIX_IN_S3,
    PMTILES_PREFIX_IN_S3,
    RUNNING_AS_TEST_OR_DEV,
    S3_ADDRESS,
    SITEMAP_INDEX,
    STAC_CATALOG_PREFIX_IN_S3,
    ZENODO_ACCESS_TOKEN,
    ZENODO_SANDBOX_ACCESS_TOKEN,
)
from userCode.lib.lakefs import LakeFSClient
from userCode.lib.stac_catalog import (
    PROCESSED_USGS_CATALOG_DESCRIPTION,
    PROCESSED_USGS_CATALOG_ID,
    PROCESSED_USGS_CATALOG_TITLE,
    SitemapDatasetMetadata,
    add_catalog_icon,
    finalize_catalog,
    init_catalog,
    init_sub_catalog,
    maplibre_style_for_pmtiles,
    multihash_sha256_file,
    patch_processed_usgs_collection,
    patch_sitemap_collection,
    register_processed_usgs_collection,
    register_sitemap_collection,
    render_thumbnail,
    sitemap_dataset_metadata,
)
from userCode.lib.utils import new_sqlalchemy_engine_from_env

"""
This file defines all geoconenx exports that move data
outside of the triplestore. 

"""

EXPORT_GROUP = "exports"


# the subset of columns in the harvested parquet that are exported as geoparquet;
# the jsonld is left out since it is large and is already exported as nquads
GEOPARQUET_COLUMNS = [
    "@id",
    "feature_name",
    "feature_description",
    "mainstem_uri",
    "geometry",
]

# target compressed size of a row group in the optimized harvested parquet;
# the low end of the 64-256MB that geoparquet-io recommends
HARVESTED_PARQUET_ROW_GROUP_TARGET_BYTES = 64 * 1024 * 1024
# the Portolan spec (rule PTL-DAT-008) limits a geoparquet row group to this many rows
HARVESTED_PARQUET_ROW_GROUP_MAX_ROWS = 150_000
# how many harvested parquet files to download from s3 at the same time
HARVESTED_PARQUET_DOWNLOAD_WORKERS = 16

DEVELOPMENT_BRANCH_IN_LAKEFS = "develop"

QLEVER_CONTAINER_NAME = "qlever.server.geoconnex"
QLEVER_DOCKER_NETWORK = "dagster_network"
QLEVER_DOCKER_ALIAS = "qlever"


def connect_qlever_to_dagster_network(timeout_seconds: int = 30) -> None:
    """Attach the QLever container to Dagster's Compose network as `qlever`."""
    client = docker.DockerClient()
    deadline = time.time() + timeout_seconds
    container = None
    last_error: Exception | None = None

    while time.time() < deadline:
        try:
            container = client.containers.get(QLEVER_CONTAINER_NAME)
            break
        except NotFound as error:
            last_error = error
            time.sleep(1)

    if container is None:
        raise RuntimeError(
            f"QLever container {QLEVER_CONTAINER_NAME!r} was not created: {last_error}"
        )

    network = client.networks.get(QLEVER_DOCKER_NETWORK)
    container.reload()
    network_config = (
        container.attrs.get("NetworkSettings", {})
        .get("Networks", {})
        .get(QLEVER_DOCKER_NETWORK)
    )
    aliases = network_config.get("Aliases", []) if network_config else []
    if QLEVER_DOCKER_ALIAS in aliases:
        return

    if network_config:
        network.disconnect(container)

    network.connect(container, aliases=[QLEVER_DOCKER_ALIAS])
    get_dagster_logger().info(
        f"Connected {QLEVER_CONTAINER_NAME} to {QLEVER_DOCKER_NETWORK} as {QLEVER_DOCKER_ALIAS}"
    )


def skip_export(context: AssetExecutionContext) -> bool:
    """Skip export if all dependencies are not materialized or we are running in test mode"""
    if not all_dependencies_materialized(context, "finished_individual_crawl"):
        get_dagster_logger().warning(
            "Skipping export as all dependencies are not materialized"
        )
        return True
    if RUNNING_AS_TEST_OR_DEV():
        get_dagster_logger().warning(
            "Dependencies are materialized, but skipping export as we are running in test mode"
        )
        return True
    return False


class ParquetConfig(Config):
    # the default location of the harvested parquet is in the assets directory
    # but this can be override for testing purposes
    harvested_parquet_directory: str = str(HARVESTED_PARQUET_DIRECTORY)
    # number of rows written to PostGIS per batch; lower this
    # if the export runs out of memory
    postgis_chunksize: int = 10_000


def harvested_sitemap_ids() -> list[str]:
    """Get the id of every sitemap which has a harvested parquet file in s3"""
    s3 = S3()
    sitemap_ids = []
    for obj in s3.client.list_objects(
        s3.bucket, prefix=STRUCTURED_PARQUET_PREFIX_IN_S3, recursive=True
    ):
        assert obj.object_name, "object_name should not be empty"
        if obj.object_name.endswith(".parquet"):
            sitemap_ids.append(Path(obj.object_name).stem)
    assert sitemap_ids, (
        f"No harvested parquet files were found under {STRUCTURED_PARQUET_PREFIX_IN_S3}"
    )
    return sorted(sitemap_ids)


@asset(
    deps=[harvest_sitemap, harvest_bulk_sitemap],
    # this is put in a separate group since it is potentially expensive
    # and thus we don't want to run it automatically
    group_name=EXPORT_GROUP,
)
def nquads_for_all_sources(config: NquadsConfig):
    """
    Generate gzipped nquads for every harvested parquet file and put them in one folder.
    Nquads are not stored in s3 so they are generated on the fly with nabu; they are kept
    on disk so the graph index can be regenerated without converting the parquet again
    """
    # start fresh so that nquads for sitemaps which were removed are not kept
    shutil.rmtree(GEOCONNEX_GRAPH_DIRECTORY, ignore_errors=True)
    GEOCONNEX_GRAPH_DIRECTORY.mkdir(parents=True)

    for sitemap_id in harvested_sitemap_ids():
        output_file = GEOCONNEX_GRAPH_DIRECTORY / f"{sitemap_id}.nq.gz"
        get_dagster_logger().info(
            f"Generating nquads for '{sitemap_id}' at {output_file.absolute()}"
        )
        NquadsContainer(sitemap_id).run(output_file, config)


def features_from_harvested_parquet(
    harvested_parquet: Path, sitemap_id: str
) -> gpd.GeoDataFrame:
    """Read the features with a geometry from a parquet file harvested by nabu"""
    gdf = gpd.read_parquet(harvested_parquet, columns=GEOPARQUET_COLUMNS)
    gdf.rename(columns={"@id": "id"}, inplace=True)
    gdf.insert(1, "geoconnex_sitemap", sitemap_id)
    return gpd.GeoDataFrame(gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty])


def _copy_rows_to_postgis(
    table: SQLTable, conn: Connection, keys: list[str], data_iter: Iterable[tuple]
) -> None:
    """
    pandas to_sql insert method that streams the rows with COPY; the same fast
    insert that GeoDataFrame.to_postgis uses
    """
    buffer = io.StringIO()
    csv.writer(buffer).writerows(data_iter)
    buffer.seek(0)

    columns = ", ".join(f'"{key}"' for key in keys)
    sql = f'COPY "{table.schema}"."{table.name}" ({columns}) FROM STDIN WITH CSV'
    # the engine uses psycopg2 so the raw dbapi cursor has copy_expert
    cursor = conn.connection.cursor()
    try:
        cursor.copy_expert(sql, buffer)
    finally:
        cursor.close()


def append_to_postgis(
    gdf: gpd.GeoDataFrame, name: str, conn: Connection, chunksize: int
) -> None:
    """
    Append a GeoDataFrame in EPSG:4326 to an existing PostGIS table.

    This is the same COPY that GeoDataFrame.to_postgis does, minus its check that
    the table's SRID from Find_SRID matches the GeoDataFrame. PostGIS 3.5 has a bug
    in the geometry_columns view (the regex for SRID check constraints lost its
    backslashes) so Find_SRID returns 0 for a column whose SRID is only enforced
    by a check constraint, which made to_postgis refuse to append. The check
    constraint on the table still rejects any geometry that is not EPSG:4326
    """
    df = pd.DataFrame(gdf)
    geom_name = gdf.geometry.name
    # EWKB embeds the SRID in each geometry so it passes the table's check constraint
    df[geom_name] = shapely.to_wkb(
        shapely.set_srid(np.asarray(gdf.geometry), 4326),
        hex=True,
        include_srid=True,
    )
    df.to_sql(
        name,
        conn,
        # the COPY quotes the schema into its SQL so it can't be left as None
        schema="public",
        if_exists="append",
        index=False,
        chunksize=chunksize,
        method=_copy_rows_to_postgis,
    )


@asset(deps=[harvest_sitemap, harvest_bulk_sitemap], group_name=EXPORT_GROUP)
def pull_harvested_parquet():
    """Download the parquet file harvested by nabu for every sitemap"""
    # start fresh so that parquet for sitemaps which were removed is not kept
    shutil.rmtree(HARVESTED_PARQUET_DIRECTORY, ignore_errors=True)
    HARVESTED_PARQUET_DIRECTORY.mkdir(parents=True)

    s3 = S3()

    def download(sitemap_id: str) -> None:
        harvested_parquet = HARVESTED_PARQUET_DIRECTORY / f"{sitemap_id}.parquet"
        get_dagster_logger().info(
            f"Downloading harvested parquet for '{sitemap_id}' to {harvested_parquet}"
        )
        s3.client.fget_object(
            s3.bucket,
            f"{STRUCTURED_PARQUET_PREFIX_IN_S3}{sitemap_id}.parquet",
            str(harvested_parquet),
        )

    # the downloads are network bound so they can run in threads; the minio client
    # is thread safe and its connection pool is larger than the number of workers
    with ThreadPoolExecutor(max_workers=HARVESTED_PARQUET_DOWNLOAD_WORKERS) as pool:
        # list() so that an exception in any download is raised here
        list(pool.map(download, harvested_sitemap_ids()))


def wrap_longitudes(geometries: np.ndarray) -> np.ndarray:
    """
    Wrap longitudes outside of [-180, 180] in an array of shapely geometries back into
    that range, in place, and return a mask of the geometries that were rewritten.
    Some sources (e.g. usgs:gnis points in the Aleutians) record a longitude like
    -182.6 instead of 177.4, which breaks consumers that validate the bbox like portolan
    """
    bounds = shapely.bounds(geometries)
    # null and empty geometries have NaN bounds so they are never out of range
    out_of_range = (bounds[:, 0] < -180) | (bounds[:, 2] > 180)
    if not out_of_range.any():
        return out_of_range

    def wrap(coords: np.ndarray) -> np.ndarray:
        coords = coords.copy()
        x = coords[:, 0]
        # only change the coordinates that are out of range since the
        # wrap arithmetic adds floating point noise to the others
        coords[:, 0] = np.where((x < -180) | (x > 180), (x + 180) % 360 - 180, x)
        return coords

    # include_z keeps the z coordinate of 3D geometries like POINT Z
    geometries[out_of_range] = shapely.transform(
        geometries[out_of_range], wrap, include_z=True
    )
    return out_of_range


def _large_schema(schema: pa.Schema) -> pa.Schema:
    """
    The same schema with the 64 bit offset large_ types for strings and binary, since
    the jsonld of large sitemaps can go over the 2GB a regular string array can hold
    """
    return pa.schema(
        [
            field.with_type(pa.large_string())
            if pa.types.is_string(field.type)
            else field.with_type(pa.large_binary())
            if pa.types.is_binary(field.type)
            else field
            for field in schema
        ],
        metadata=schema.metadata,
    )


def optimize_harvested_parquet_file(harvested_parquet: Path, sitemap_id: str) -> None:
    """
    Rewrite a harvested parquet file in place following geoparquet best practices:
    wrap out of range longitudes, sort the rows along a hilbert curve so spatially
    close features are in the same row groups, and use row groups of a reasonable size
    (nabu writes very small ones). The schema nabu wrote is kept as is so the file
    can still be read by nabu and everything else downstream.

    Large sitemaps like usgs:national_geologic_map don't fit in memory, so the file
    is streamed: the sort order is computed from the geometry bounding boxes alone,
    then the rows are spread into one spill file per output row group, and each spill
    file is sorted and written as a row group. Only one batch is in memory at a time
    """
    parquet_file = pq.ParquetFile(harvested_parquet)
    num_rows = parquet_file.metadata.num_rows
    # nabu writes the geoparquet metadata to the parquet footer but not to the
    # arrow schema, so it has to be carried over explicitly
    file_metadata = parquet_file.metadata.metadata or {}
    geo_metadata = {b"geo": file_metadata[b"geo"]} if b"geo" in file_metadata else {}
    schema = parquet_file.schema_arrow.with_metadata(geo_metadata)
    large_schema = _large_schema(schema)
    geometry_index = schema.get_field_index("geometry")

    # nabu also compresses with zstd so the current file size is a good estimate;
    # spread the rows evenly so there isn't a small leftover row group at the end;
    # small features like points can hit the row limit before the size target
    num_groups = max(
        1,
        round(
            harvested_parquet.stat().st_size / HARVESTED_PARQUET_ROW_GROUP_TARGET_BYTES
        ),
        math.ceil(num_rows / HARVESTED_PARQUET_ROW_GROUP_MAX_ROWS),
    )
    rows_per_group = max(1, math.ceil(num_rows / num_groups))
    num_groups = math.ceil(num_rows / rows_per_group)

    # first pass: only read the geometry and keep its envelope, which has the same
    # bounding box (and so the same position on the hilbert curve) as the full
    # geometry but is much smaller than a complex polygon
    envelopes = []
    out_of_range = []
    out_of_range_ids = []
    for batch in parquet_file.iter_batches(
        batch_size=rows_per_group, columns=["geometry", "@id"]
    ):
        geometries = shapely.from_wkb(
            batch.column("geometry").to_numpy(zero_copy_only=False)
        )
        batch_out_of_range = wrap_longitudes(geometries)
        out_of_range.append(batch_out_of_range)
        if batch_out_of_range.any():
            ids = batch.column("@id").to_numpy(zero_copy_only=False)
            out_of_range_ids.extend(ids[batch_out_of_range].tolist())
        envelopes.append(shapely.to_wkb(shapely.envelope(geometries)))
    out_of_range_mask = (
        np.concatenate(out_of_range) if out_of_range else np.zeros(0, dtype=bool)
    )
    envelope_wkb = pa.array(
        np.concatenate(envelopes) if envelopes else [], type=pa.binary()
    )
    del envelopes, out_of_range

    if out_of_range_ids:
        get_dagster_logger().warning(
            f"Sitemap '{sitemap_id}' has {len(out_of_range_ids)} features with a "
            f"longitude outside of [-180, 180]; wrapping them back into range. "
            f"Affected ids: {out_of_range_ids}"
        )

    order = np.arange(num_rows)
    if envelope_wkb.null_count < num_rows:
        # sort only the envelopes and a row index so the large jsonld column isn't
        # copied around and the schema nabu wrote (which hilbert_order_table can
        # change) is kept as is
        row_index = pa.table(
            {"geometry": envelope_wkb, "row": order}, metadata=geo_metadata
        )
        sorted_index = gpio.ops.hilbert_order_table(
            row_index, geometry_column="geometry"
        )
        order = sorted_index.column("row").to_numpy()
    del envelope_wkb

    # the position of every input row in the output
    destination = np.empty(num_rows, dtype=np.int64)
    destination[order] = np.arange(num_rows)
    del order

    optimized_parquet = harvested_parquet.with_suffix(".parquet.tmp")
    # spill to the same disk as the parquet since it has room for a copy of it
    with tempfile.TemporaryDirectory(dir=harvested_parquet.parent) as spill_dir:
        spill_files = [
            Path(spill_dir) / f"{group}.arrow" for group in range(num_groups)
        ]
        spill_schema = large_schema.append(pa.field("__destination", pa.int64()))

        # second pass: send every row to the spill file of its output row group
        # compress the spill files since uncompressed jsonld is many times its
        # size in the parquet
        spill_options = pa.ipc.IpcWriteOptions(compression="zstd")
        spill_writers = [
            pa.ipc.new_stream(str(spill_file), spill_schema, options=spill_options)
            for spill_file in spill_files
        ]
        try:
            start = 0
            for batch in parquet_file.iter_batches(batch_size=rows_per_group):
                stop = start + batch.num_rows
                table = pa.Table.from_batches([batch])
                batch_out_of_range = out_of_range_mask[start:stop]
                if batch_out_of_range.any():
                    wkb = table.column("geometry").to_numpy(zero_copy_only=False)
                    geometries = shapely.from_wkb(wkb[batch_out_of_range])
                    wrap_longitudes(geometries)
                    wkb[batch_out_of_range] = shapely.to_wkb(geometries)
                    table = table.set_column(
                        geometry_index,
                        schema.field("geometry"),
                        pa.array(wkb, type=schema.field("geometry").type),
                    )
                batch_destination = destination[start:stop]
                groups = batch_destination // rows_per_group
                by_group = np.argsort(groups, kind="stable")
                table = (
                    table.cast(large_schema.with_metadata(None))
                    .append_column("__destination", pa.array(batch_destination))
                    .take(by_group)
                )
                groups = groups[by_group]
                boundaries = np.flatnonzero(np.diff(groups)) + 1
                for group_start, group_stop in zip(
                    np.concatenate([[0], boundaries]),
                    np.concatenate([boundaries, [len(groups)]]),
                    strict=True,
                ):
                    spill_writers[int(groups[group_start])].write_table(
                        table.slice(group_start, group_stop - group_start)
                    )
                start = stop
        finally:
            for spill_writer in spill_writers:
                spill_writer.close()

        # third pass: sort each spill file and write it as one row group; write to a
        # temporary file first so a failure doesn't leave a partial file behind
        with pq.ParquetWriter(optimized_parquet, schema, compression="zstd") as writer:
            for spill_file in spill_files:
                with pa.ipc.open_stream(str(spill_file)) as reader:
                    rows = reader.read_all()
                spill_file.unlink()
                rows = rows.take(np.argsort(rows.column("__destination").to_numpy()))
                # each write is its own row group since it is under the default
                # row group size
                writer.write_table(rows.drop_columns("__destination").cast(schema))
    optimized_parquet.replace(harvested_parquet)


@asset(deps=[pull_harvested_parquet], group_name=EXPORT_GROUP)
def optimize_harvested_parquet():
    """
    Optimize the harvested parquet for every sitemap and upload it back to s3 so
    the files published in the STAC catalog match the ones used for the exports
    """
    s3 = S3()
    for harvested_parquet in sorted(HARVESTED_PARQUET_DIRECTORY.glob("*.parquet")):
        sitemap_id = harvested_parquet.stem
        get_dagster_logger().info(f"Optimizing harvested parquet for '{sitemap_id}'")
        optimize_harvested_parquet_file(harvested_parquet, sitemap_id)
        s3.client.fput_object(
            s3.bucket,
            f"{STRUCTURED_PARQUET_PREFIX_IN_S3}{sitemap_id}.parquet",
            str(harvested_parquet),
        )


@asset(deps=[optimize_harvested_parquet], group_name=EXPORT_GROUP)
def pmtiles_from_harvested_parquet():
    """
    Generate one pmtiles file per sitemap that represents all locations in the Geoconnex graph
    """
    pmtiles_dir = ASSETS_DIRECTORY / "pmtiles"

    # clear out old outputs so that removed sitemaps are not uploaded
    shutil.rmtree(pmtiles_dir, ignore_errors=True)
    pmtiles_dir.mkdir(parents=True)

    pmtiles_to_sitemap_id: dict[Path, str] = {}
    with tempfile.TemporaryDirectory() as features_dir:
        for harvested_parquet in sorted(HARVESTED_PARQUET_DIRECTORY.glob("*.parquet")):
            sitemap_id = harvested_parquet.stem
            gdf = features_from_harvested_parquet(harvested_parquet, sitemap_id)
            if gdf.empty:
                get_dagster_logger().warning(
                    f"Sitemap '{sitemap_id}' has no features with a geometry; skipping"
                )
                continue
            # sitemap ids may contain characters that are not safe for filenames
            sanitized_name = sanitize_filename(sitemap_id)
            # only write the columns needed for the tiles so the
            # large jsonld column is not included in them
            features_file = Path(features_dir) / f"{sanitized_name}.parquet"
            gdf.to_parquet(features_file)

            pmtiles_file = pmtiles_dir / f"{sanitized_name}.pmtiles"
            get_dagster_logger().info(f"Generating pmtiles for sitemap '{sitemap_id}'")
            # requires tippecanoe to be installed and on the PATH
            gpio.ops.create_pmtiles(
                str(features_file),
                str(pmtiles_file),
                force=True,
            )
            pmtiles_to_sitemap_id[pmtiles_file] = sitemap_id

    assert pmtiles_to_sitemap_id, f"No pmtiles files were generated in {pmtiles_dir}"

    if RUNNING_AS_TEST_OR_DEV():
        get_dagster_logger().warning("Skipping export as we are running in test mode")
        return

    s3 = S3()

    for pmtiles_file, sitemap_id in pmtiles_to_sitemap_id.items():
        # the s3 client url encodes the object key, so the raw sitemap id
        # can be used both here and when fetching the object
        remote_path = f"{EXPORTS_PREFIX_IN_S3}pmtiles/{sitemap_id}.pmtiles"
        get_dagster_logger().info(
            f"Uploading {remote_path} of size {pmtiles_file.stat().st_size} to bucket '{s3.bucket}' in the object store"
        )
        with pmtiles_file.open("rb") as f:
            s3.load_stream(
                stream=f,
                remote_path=remote_path,
                content_length=pmtiles_file.stat().st_size,
                content_type="application/vnd.pmtiles",
                headers={},
            )


def _guess_content_type(file: Path) -> str:
    """Guess the content type of a generated STAC catalog file"""
    if file.suffix == ".md":
        return "text/markdown"
    content_type, _ = mimetypes.guess_type(file.name)
    return content_type or "application/octet-stream"


def add_processed_usgs_catalog(catalog_root: Path, s3: S3) -> None:
    """
    Add the processed USGS data products sub-catalog to the STAC catalog, with a
    collection for the reference catchments flatgeobuf the processed asset group
    publishes; skipped if that file has not been generated yet
    """
    try:
        metadata = json.loads(s3.read(REFERENCE_CATCHMENTS_METADATA_PATH_IN_S3))
    except S3Error as e:
        if e.code != "NoSuchKey":
            raise
        get_dagster_logger().warning(
            f"{REFERENCE_CATCHMENTS_METADATA_PATH_IN_S3} is not in the object store; "
            f"skipping the '{PROCESSED_USGS_CATALOG_TITLE}' catalog"
        )
        return

    init_sub_catalog(
        catalog_root,
        catalog_id=PROCESSED_USGS_CATALOG_ID,
        title=PROCESSED_USGS_CATALOG_TITLE,
        description=PROCESSED_USGS_CATALOG_DESCRIPTION,
    )
    collection_path = register_processed_usgs_collection(
        catalog_root,
        name=REFERENCE_CATCHMENTS_NAME,
        data_url=f"https://{S3_ADDRESS}/{s3.bucket}/{REFERENCE_CATCHMENTS_PATH_IN_S3}",
        title=REFERENCE_CATCHMENTS_TITLE,
        description=REFERENCE_CATCHMENTS_DESCRIPTION,
        bbox=metadata["bbox"],
    )
    thumbnail_filename = "thumbnail.png"
    (collection_path.parent / thumbnail_filename).write_bytes(
        s3.read(REFERENCE_CATCHMENTS_THUMBNAIL_PATH_IN_S3)
    )
    patch_processed_usgs_collection(
        collection_path,
        data_size=metadata["file:size"],
        data_checksum=metadata["file:checksum"],
        table_columns=REFERENCE_CATCHMENTS_TABLE_COLUMNS,
        sources=metadata["sources"],
        thumbnail_filename=thumbnail_filename,
    )


@asset(deps=[pmtiles_from_harvested_parquet], group_name=EXPORT_GROUP)
def publish_stac_catalog():
    """
    Build a Portolan-formatted STAC catalog (https://github.com/portolan-sdi/portolan-spec)
    describing every geoparquet and pmtiles file this export has published, and publish
    it to the object store alongside the data it describes so it is served statically.
    """
    if RUNNING_AS_TEST_OR_DEV():
        get_dagster_logger().warning(
            "Skipping STAC catalog publish as we are running in test mode"
        )
        return

    logger = get_dagster_logger()
    s3 = S3()

    parquet_sizes: dict[str, int] = {}
    for obj in s3.client.list_objects(
        s3.bucket, prefix=STRUCTURED_PARQUET_PREFIX_IN_S3, recursive=True
    ):
        if obj.object_name and obj.object_name.endswith(".parquet"):
            parquet_sizes[Path(obj.object_name).stem] = obj.size or 0

    assert parquet_sizes, (
        f"No geoparquet files were found under {STRUCTURED_PARQUET_PREFIX_IN_S3}; "
        "cannot build a STAC catalog"
    )

    pmtiles_sizes: dict[str, int] = {}
    for obj in s3.client.list_objects(
        s3.bucket, prefix=PMTILES_PREFIX_IN_S3, recursive=True
    ):
        if obj.object_name and obj.object_name.endswith(".pmtiles"):
            pmtiles_sizes[Path(obj.object_name).stem] = obj.size or 0

    sitemap_index_response = requests.get(SITEMAP_INDEX, timeout=20)
    sitemap_index_response.raise_for_status()
    dataset_metadata = sitemap_dataset_metadata(sitemap_index_response.text)

    with tempfile.TemporaryDirectory() as tmp:
        catalog_root = Path(tmp) / "catalog"
        catalog_root.mkdir()
        init_catalog(catalog_root)
        add_catalog_icon(catalog_root)

        for sitemap_id in sorted(parquet_sizes):
            local_parquet = HARVESTED_PARQUET_DIRECTORY / f"{sitemap_id}.parquet"
            if not local_parquet.exists():
                logger.warning(
                    f"Harvested parquet for '{sitemap_id}' is listed in s3 but not "
                    f"present locally at {local_parquet}; skipping its STAC collection"
                )
                continue

            gdf = features_from_harvested_parquet(local_parquet, sitemap_id)
            if gdf.empty:
                logger.warning(
                    f"Sitemap '{sitemap_id}' has no features with a geometry; "
                    "skipping its STAC collection"
                )
                continue

            bbox = [round(float(v), 6) for v in gdf.total_bounds.tolist()]
            parquet_url = (
                f"https://{S3_ADDRESS}/{s3.bucket}/"
                f"{STRUCTURED_PARQUET_PREFIX_IN_S3}{sitemap_id}.parquet"
            )

            rdf_parquet_url = (
                f"https://{S3_ADDRESS}/{s3.bucket}/"
                f"{RDF_PARQUET_PREFIX_IN_S3}{sitemap_id}.parquet"
            )

            metadata = dataset_metadata.get(sitemap_id, SitemapDatasetMetadata())
            collection_path = register_sitemap_collection(
                catalog_root,
                sitemap_id=sitemap_id,
                parquet_url=parquet_url,
                bbox=bbox,
                dataset_name=metadata.name,
                dataset_description=metadata.description,
                dataset_documentation_link=metadata.documentation_link,
                source_code_link=metadata.source_code_link,
                add_associated_mainstems=metadata.add_associated_mainstems,
            )
            collection_dir = collection_path.parent

            # the pmtiles file is keyed by the raw sitemap id in s3, same as the
            # parquet, but tippecanoe names the layer inside it after the
            # sanitized local filename it was built from (see
            # pmtiles_from_harvested_parquet above)
            pmtiles_url = None
            pmtiles_layer = None
            if sitemap_id in pmtiles_sizes:
                pmtiles_layer = sanitize_filename(sitemap_id)
                pmtiles_url = (
                    f"https://{S3_ADDRESS}/{s3.bucket}/"
                    f"{PMTILES_PREFIX_IN_S3}{sitemap_id}.pmtiles"
                )

            thumbnail_filename = "thumbnail.png"
            render_thumbnail(gdf, collection_dir / thumbnail_filename)

            style_filename = None
            if pmtiles_url and pmtiles_layer:
                style_filename = "style.json"
                (collection_dir / style_filename).write_text(
                    json.dumps(maplibre_style_for_pmtiles(pmtiles_url, pmtiles_layer))
                )

            patch_sitemap_collection(
                collection_path,
                sitemap_id=sitemap_id,
                parquet_size=parquet_sizes[sitemap_id],
                parquet_checksum=multihash_sha256_file(local_parquet),
                rdf_parquet_url=rdf_parquet_url,
                pmtiles_url=pmtiles_url,
                pmtiles_layer=pmtiles_layer,
                thumbnail_filename=thumbnail_filename,
                style_filename=style_filename,
            )

        add_processed_usgs_catalog(catalog_root, s3)

        finalize_catalog(catalog_root)

        logger.info(
            f"Deleting any existing STAC catalog files under {STAC_CATALOG_PREFIX_IN_S3}"
        )
        s3.remove_prefix(STAC_CATALOG_PREFIX_IN_S3)

        for file in sorted(catalog_root.rglob("*")):
            # the .portolan directory holds internal tool state (e.g. the raw
            # s3 endpoint), not public catalog content, so it is never published
            if not file.is_file() or ".portolan" in file.parts:
                continue
            remote_path = (
                f"{STAC_CATALOG_PREFIX_IN_S3}"
                f"{file.relative_to(catalog_root).as_posix()}"
            )
            logger.info(f"Uploading {remote_path} to the object store")
            with file.open("rb") as f:
                s3.load_stream(
                    stream=f,
                    remote_path=remote_path,
                    content_length=file.stat().st_size,
                    content_type=_guess_content_type(file),
                    headers={},
                )


@asset(
    deps=[nquads_for_all_sources],
    # this is put in a separate group since it is potentially expensive
    # and thus we don't want to run it automatically
    group_name=EXPORT_GROUP,
)
def qlever_index():
    """
    Generate the qlever index
    """
    logger = get_dagster_logger()

    original_cwd = Path.cwd()
    os.chdir(ASSETS_DIRECTORY)

    try:
        # overwrite existing index if it exists and use up to 11GB of memory for building the
        # index; otherwise qlever will only use the default of 1GB
        qlever_cmd = [
            "qlever",
            "index",
            "--overwrite-existing",
            "--stxxl-memory",
            "11GB",
        ]

        process = subprocess.Popen(
            qlever_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # merge streams
            text=True,
            bufsize=1,
        )
        assert process.stdout, (
            "no stdout present for process; this is a sign something didn't launch properly"
        )
        for line in process.stdout:
            line = line.rstrip()
            if not line:
                continue
            if "WARN" in line:
                logger.warning(line)
            else:
                logger.info(line)
        returncode = process.wait()
        if returncode != 0:
            raise RuntimeError("qlever index generation failed")

        get_dagster_logger().info("qlever index generation complete")

        GEOCONNEX_INDEX_DIRECTORY.mkdir(exist_ok=True)

        # move all geoconnex.* files to geoconnex_index directory for cleanliness
        for path in ASSETS_DIRECTORY.iterdir():
            if path.is_file() and path.name.startswith("geoconnex."):
                path.rename(GEOCONNEX_INDEX_DIRECTORY / path.name)
    finally:
        os.chdir(original_cwd)


@asset_check(asset=qlever_index, blocking=True)
def geoconnex_sparql_query_check() -> AssetCheckResult:
    """
    Ensure that all queries pass the Geoconnex SPARQL query check,
    preventing any regressions with new data
    """
    queries = list((Path(__file__).parent / "queries").iterdir())
    original_cwd = Path.cwd()

    start_qlever_cmd = [
        "qlever",
        "--qleverfile",
        str(ASSETS_DIRECTORY / "Qleverfile"),
        "start",
        "--run-in-foreground",
        "--kill-existing-with-same-port",
    ]

    process = subprocess.Popen(
        start_qlever_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # merge streams
        text=True,
        bufsize=1,
        cwd=GEOCONNEX_INDEX_DIRECTORY,
    )

    try:
        if not RUNNING_AS_TEST_OR_DEV():
            connect_qlever_to_dagster_network()

        endpoint = (
            "http://localhost:8888"
            if RUNNING_AS_TEST_OR_DEV()
            else "http://qlever:8888"
        )
        last_error: Exception | None = None
        for _ in range(30):
            try:
                requests.get(endpoint, timeout=1)
                last_error = None
                break
            except requests.RequestException as error:
                last_error = error
                time.sleep(1)
        if last_error:
            return AssetCheckResult(
                passed=False,
                description=f"QLever server did not become ready: {last_error}",
                severity=AssetCheckSeverity.ERROR,
            )

        for file in queries:
            get_dagster_logger().info(f"Checking {file.name}")
            result = requests.post(
                endpoint,
                headers={
                    "Accept": "application/sparql-results+json",
                },
                data={"query": file.read_text()},
                timeout=30,
            )
            if result.status_code > 300:
                return AssetCheckResult(
                    passed=False,
                    description=f"Query {file.name} failed with status code {result.status_code} and body {result.text}",
                    severity=AssetCheckSeverity.ERROR,
                )
            as_json = result.json()
            assert "boolean" in as_json, (
                f"ASK Query {file.name} did not return a boolean"
            )

            if not as_json["boolean"]:
                return AssetCheckResult(
                    passed=False,
                    description=f"Geoconnex SPARQL query '{file.name}' failed",
                    severity=AssetCheckSeverity.ERROR,
                )

        return AssetCheckResult(
            passed=True,
            description="Geoconnex SPARQL query check passed",
        )
    finally:
        # clean up qlever process
        process.kill()
        process.wait()
        stdout = process.stdout
        stderr = process.stderr
        if stdout:
            stdout.close()
        if stderr:
            stderr.close()
        os.chdir(original_cwd)


@asset(
    deps=[nquads_for_all_sources],
    # this is put in a separate group since it is potentially expensive
    # and thus we don't want to run it automatically
    group_name=EXPORT_GROUP,
)
def oci_artifact():
    """
    Upload the Geoconnex graph as an OCI artifact to Github Container Registry
    """
    os.chdir(GEOCONNEX_GRAPH_DIRECTORY)
    date_str = datetime.now().strftime("%Y_%m_%d")
    tags = f"{date_str},latest"

    registry = "localhost:5000" if RUNNING_AS_TEST_OR_DEV() else "ghcr.io"

    files_to_upload = []
    for file in GEOCONNEX_GRAPH_DIRECTORY.iterdir():
        if file.name.endswith(".nq.gz") or file.name.endswith(".nq"):
            relative_path = file.relative_to(GEOCONNEX_GRAPH_DIRECTORY)
            files_to_upload.append(f"{relative_path}:application/n-quads")

    command = f"oras push {registry}/internetofwater/geoconnex-graph:{tags} {' '.join(files_to_upload)} --username internetofwater --password-stdin --annotation 'org.opencontainers.image.description=All RDF data in NQuad format which makes up the Geoconnex Graph as of the date in the image tag' --annotation 'org.opencontainers.image.source=https://github.com/internetofwater/geoconnex.us'"

    logger = get_dagster_logger()
    logger.info(f"Running '{command}'")

    # Use shell=True so the command string is interpreted correctly
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        stdin=subprocess.PIPE,
        shell=True,
    )

    assert process.stdout, "No stdout present; process didn't launch properly"
    assert process.stdin, "No stdin present; process didn't launch properly"

    process.stdin.write(GHCR_TOKEN)
    process.stdin.close()

    for line in process.stdout:
        line = line.rstrip()
        if not line:
            continue
        if "WARN" in line:
            logger.warning(line)
        else:
            logger.info(line)

    returncode = process.wait()
    if returncode != 0:
        raise RuntimeError("ORAS push failed")
    get_dagster_logger().info("Pushing qlever index to registry")

    # restore the dir
    os.chdir(os.path.dirname(__file__))


@asset(
    group_name=EXPORT_GROUP,
    deps=[qlever_index],
    automation_condition=AutomationCondition.eager(),
)
def stream_qlever_index_to_gcs(context: AssetExecutionContext):
    """
    Stream all files in the generated qlever index to GCS
    """
    if RUNNING_AS_TEST_OR_DEV():
        get_dagster_logger().warning("Skipping export as we are running in test mode")
        return

    s3 = S3()
    existing_objects = s3.client.list_objects(
        prefix="geoconnex_index/", bucket_name=s3.bucket, recursive=True
    )
    get_dagster_logger().info("Deleting existing old index files in geoconnex_index/")
    for obj in existing_objects:
        get_dagster_logger().info(f"Deleting {obj}")
        assert obj.object_name, f"obj name should not be empty but got '{obj}'"
        s3.client.remove_object(object_name=obj.object_name, bucket_name=s3.bucket)
    get_dagster_logger().info("Finished deleting old index files")
    for file in GEOCONNEX_INDEX_DIRECTORY.iterdir():
        if not file.is_file():
            raise Exception(f"{file} is not a file and thus cannot be uploaded")

        get_dagster_logger().info(
            f"Uploading {file.name} of size {file.stat().st_size} to the object store"
        )
        with file.open("rb") as f:
            s3.load_stream(
                stream=f,
                remote_path=f"geoconnex_index/{file.name}",
                content_length=file.stat().st_size,
                content_type="octet-stream",
                headers={},
            )


@asset(
    group_name=EXPORT_GROUP,
    deps=[optimize_harvested_parquet],
)
def move_geoparquet_to_postgis(config: ParquetConfig):
    """
    Load the harvested parquet for every sitemap and write it into PostGIS using GeoPandas to_postgis.
    """

    engine = new_sqlalchemy_engine_from_env()

    harvested_parquet_files = sorted(
        Path(config.harvested_parquet_directory).glob("*.parquet")
    )
    assert harvested_parquet_files, (
        f"No harvested parquet files were found in {config.harvested_parquet_directory}"
    )

    # Do the whole load (table replace, appends, and indexes) in a single transaction
    # so a failure part way through rolls back and leaves the previous table intact
    with engine.begin() as conn:
        # first pass through, if exists, replace. Otherwise append to the table
        # since that means we just overwrite the table and are ready to add to it
        if_exists: Literal["replace", "append"] = "replace"
        for harvested_parquet in harvested_parquet_files:
            sitemap_id = harvested_parquet.stem
            get_dagster_logger().info(
                f"Moving geoparquet data for '{sitemap_id}' from {harvested_parquet} to PostGIS"
            )
            # load one sitemap at a time so we don't run out of memory
            gdf = features_from_harvested_parquet(harvested_parquet, sitemap_id)
            if gdf.empty:
                # some sitemaps (e.g. epa:hmw) have no geometry at all; there is
                # nothing to load for them so don't touch the table
                get_dagster_logger().warning(
                    f"Sitemap '{sitemap_id}' has no features with a geometry; skipping"
                )
                continue
            gdf.set_crs(epsg=4326, inplace=True, allow_override=True)

            if if_exists == "append":
                append_to_postgis(
                    gdf, "geoconnex_features", conn, config.postgis_chunksize
                )
            else:
                gdf.to_postgis(
                    name="geoconnex_features",
                    con=conn,
                    if_exists=if_exists,
                    # do not add the pandas index as a separate column
                    index=False,
                    schema=None,
                    # write in chunks so we don't run out of memory
                    chunksize=config.postgis_chunksize,
                )
                # to_postgis types the geometry column from only the first sitemap's
                # geometries (e.g. POINT); widen it so later sitemaps with other
                # geometry types (e.g. POLYGON) or dimensions (2D vs 3D) can be appended.
                # A typmod like geometry(Geometry, 4326) would still pin the column to 2D,
                # so drop the typmod entirely and enforce the SRID with a check constraint
                # instead. Appends go through append_to_postgis since the SRID from
                # the constraint is not reported correctly on all PostGIS versions
                conn.execute(
                    text(
                        "ALTER TABLE geoconnex_features "
                        "ALTER COLUMN geometry TYPE geometry, "
                        "ADD CONSTRAINT enforce_srid_geometry "
                        "CHECK (ST_SRID(geometry) = 4326);"
                    )
                )
            if_exists = "append"

        assert if_exists == "append", (
            "None of the harvested parquet files contained features with a geometry"
        )

        # new count in postgis
        result = conn.execute(text("SELECT count(*) FROM geoconnex_features;"))
        count = result.scalar()

        get_dagster_logger().info("Creating indexes on geoconnex_features table")
        get_dagster_logger().info("Creating indexes on id property")
        conn.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_geoconnex_features_id
            ON geoconnex_features (id);
        """)
        )

        get_dagster_logger().info("Creating indexes on geoconnex_sitemap property")
        conn.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_geoconnex_sitemap
            ON geoconnex_features (geoconnex_sitemap);
            """)
        )

        get_dagster_logger().info("Creating indexes on mainstem_uri property")
        conn.execute(
            text(
                """CREATE INDEX IF NOT EXISTS idx_mainstem_uri ON geoconnex_features (mainstem_uri);"""
            )
        )

        get_dagster_logger().info(
            "Creating trigram index on feature_name property for fuzzy text search"
        )
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
        conn.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_geoconnex_features_feature_name_trgm
            ON geoconnex_features USING GIN (feature_name gin_trgm_ops);

            CREATE INDEX IF NOT EXISTS idx_geoconnex_features_feature_name
            ON geoconnex_features (feature_name);
            """)
        )

    get_dagster_logger().info(
        f"Finishing moving Parquet data into postgis. Table 'geoconnex_features' now has {count} rows."
    )


@asset(
    group_name=EXPORT_GROUP,
    deps=[nquads_for_all_sources],
)
def stream_all_nquads_to_renci(
    context: AssetExecutionContext,
    rclone_config: str,
):
    """
    Stream the nquads for all sitemaps to RENCI
    """
    if RUNNING_AS_TEST_OR_DEV():
        get_dagster_logger().warning("Skipping export as we are running in test mode")
        return
    lakefs_client = LakeFSClient("geoconnex")
    get_dagster_logger().info(
        f"Uploading nquads from {GEOCONNEX_GRAPH_DIRECTORY} to lakefs at {DEVELOPMENT_BRANCH_IN_LAKEFS}"
    )
    RcloneClient(rclone_config).copy_directory_to_lakefs(
        destination_branch=DEVELOPMENT_BRANCH_IN_LAKEFS,
        source_directory=GEOCONNEX_GRAPH_DIRECTORY,
        lakefs_client=lakefs_client,
    )


@asset(group_name=EXPORT_GROUP, deps=[nquads_for_all_sources])
def stream_nquads_to_zenodo(
    context: AssetExecutionContext,
):
    """Upload nquads to Zenodo as a new deposit"""
    # check if we are running in test mode and thus want to upload to the sandbox
    SANDBOX_MODE = (
        ZENODO_SANDBOX_ACCESS_TOKEN != "unset" and "PYTEST_CURRENT_TEST" in os.environ
    )

    if (
        RUNNING_AS_TEST_OR_DEV()
        # if we are running against a test sandbox, allow the user to upload
        and not SANDBOX_MODE
    ):
        return

    ZENODO_API_URL = (
        "https://zenodo.org/api/deposit/depositions"
        if not SANDBOX_MODE
        else "https://sandbox.zenodo.org/api/deposit/depositions"
    )

    if SANDBOX_MODE:
        ZENODO_API_URL = "https://sandbox.zenodo.org/api/deposit/depositions"
        TOKEN = ZENODO_SANDBOX_ACCESS_TOKEN
    else:
        ZENODO_API_URL = "https://zenodo.org/api/deposit/depositions"
        TOKEN = ZENODO_ACCESS_TOKEN

    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
    }

    # Create a new deposit (this is essentially akin to a commit
    # that groups multiple file additions together in an update request
    response = requests.post(ZENODO_API_URL, json={}, headers=headers, timeout=30)
    response.raise_for_status()
    deposit = response.json()

    # Extract Deposit ID
    deposit_id = deposit["id"]
    get_dagster_logger().info(f"Deposit created with ID: {deposit_id}")

    if not GEOCONNEX_GRAPH_DIRECTORY.exists():
        raise Exception(
            f"{GEOCONNEX_GRAPH_DIRECTORY} does not exist and thus the nquads cannot be uploaded"
        )

    # Read file stream from the local nquads
    # we are not decoding the content to upsert it as gzip to zenodo
    for i, graph in enumerate(GEOCONNEX_GRAPH_DIRECTORY.iterdir()):
        if not graph.is_file():
            raise Exception(f"{graph} is not a file and thus cannot be uploaded")

        if graph.name.endswith(".bytesum"):
            # skip bytesum hash files
            continue

        if not graph.name.endswith(".nq.gz") and not graph.name.endswith(".nq"):
            # warn but don't error
            get_dagster_logger().warning(
                f"Found unexpected file: {graph.name}, which is not a .nq or .nq.gz file; skipping..."
            )
            continue

        with graph.open("rb") as f:
            get_dagster_logger().info(
                f"Uploading {graph.name} #{i} of size {graph.stat().st_size} bytes to Zenodo"
            )
            # Use the deposit ID to upload the file
            TWENTY_MINUTES = 60 * 20
            response = requests.put(
                f"{deposit['links']['bucket']}/{graph.name}",
                data=f,
                headers={"Authorization": f"Bearer {TOKEN}"},
                timeout=TWENTY_MINUTES,
            )
            response.raise_for_status()

    get_dagster_logger().info("All files uploaded successfully.")

    # Add metadata to the upload
    metadata = {
        "metadata": {
            "title": "Geoconnex Graph",
            "upload_type": "dataset",
            "description": (
                "These files file represent the n-quads export of all RDF data in each sitemap, "
                "which makes up the Geoconnex graph database. Documentation "
                "and background can be found at https://docs.geoconnex.us"
            ),
            "creators": [
                {
                    "name": "Internet of Water Coalition",
                    "affiliation": "Internet of Water Coalition",
                }
            ],
        }
    }

    metadata_url = f"{ZENODO_API_URL}/{deposit_id}"
    response = requests.put(metadata_url, json=metadata, headers=headers, timeout=30)
    response.raise_for_status()

    get_dagster_logger().info(f"Metadata updated for deposit ID {deposit_id}")

    """
    In zenodo you cannot delete a deposit after it has been published.
    Thus, the code below is commented out. It is safer not to automatically
    publish the deposit. However the code below is tested and works.
    """
    # publish the deposit; thus making it no longer tagged as a draft
    # publish_url = f"{ZENODO_API_URL}/{deposit_id}/actions/publish"
    # response = requests.post(publish_url, headers=headers)
    # response.raise_for_status()
    # get_dagster_logger().info("Deposit published successfully.")
    # return deposit_id


@asset(group_name=EXPORT_GROUP, deps=[stream_all_nquads_to_renci])
def merge_lakefs_branch_into_main(context: AssetExecutionContext):
    """
    Manually merge the develop branch into the main branch
    the renci lakefs. This is done as a separate step to avoid
    auto merging unfinished or incorrect assets until they have been
    checked
    """
    if RUNNING_AS_TEST_OR_DEV():
        get_dagster_logger().warning("Skipping export as we are running in test mode")
        return
    LakeFSClient("geoconnex").merge_branch_into_main(branch="develop")
