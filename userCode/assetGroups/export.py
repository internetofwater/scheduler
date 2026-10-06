# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


from datetime import datetime
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
import requests
from sqlalchemy import text

from userCode.assetGroups.harvest import harvest_sitemap
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
    HARVESTED_PARQUET_PREFIX_IN_S3,
    RUNNING_AS_TEST_OR_DEV,
    ZENODO_ACCESS_TOKEN,
    ZENODO_SANDBOX_ACCESS_TOKEN,
)
from userCode.lib.lakefs import LakeFSClient
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
        s3.bucket, prefix=HARVESTED_PARQUET_PREFIX_IN_S3, recursive=True
    ):
        assert obj.object_name, "object_name should not be empty"
        if obj.object_name.endswith(".parquet"):
            sitemap_ids.append(Path(obj.object_name).stem)
    assert sitemap_ids, (
        f"No harvested parquet files were found under {HARVESTED_PARQUET_PREFIX_IN_S3}"
    )
    return sorted(sitemap_ids)


@asset(
    deps=[harvest_sitemap],
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


@asset(deps=[harvest_sitemap], group_name=EXPORT_GROUP)
def pull_harvested_parquet():
    """Download the parquet file harvested by nabu for every sitemap"""
    # start fresh so that parquet for sitemaps which were removed is not kept
    shutil.rmtree(HARVESTED_PARQUET_DIRECTORY, ignore_errors=True)
    HARVESTED_PARQUET_DIRECTORY.mkdir(parents=True)

    s3 = S3()
    for sitemap_id in harvested_sitemap_ids():
        harvested_parquet = HARVESTED_PARQUET_DIRECTORY / f"{sitemap_id}.parquet"
        get_dagster_logger().info(
            f"Downloading harvested parquet for '{sitemap_id}' to {harvested_parquet}"
        )
        s3.client.fget_object(
            s3.bucket,
            f"{HARVESTED_PARQUET_PREFIX_IN_S3}{sitemap_id}.parquet",
            str(harvested_parquet),
        )


@asset(deps=[pull_harvested_parquet], group_name=EXPORT_GROUP)
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
    automation_condition=AutomationCondition.eager(),
    deps=[pull_harvested_parquet],
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

    # replace the table with the first sitemap and append the rest
    if_exists: Literal["replace", "append"] = "replace"
    for harvested_parquet in harvested_parquet_files:
        sitemap_id = harvested_parquet.stem
        get_dagster_logger().info(
            f"Moving geoparquet data for '{sitemap_id}' from {harvested_parquet} to PostGIS"
        )
        # load one sitemap at a time so we don't run out of memory
        gdf = features_from_harvested_parquet(harvested_parquet, sitemap_id)
        gdf.set_crs(epsg=4326, inplace=True, allow_override=True)

        gdf.to_postgis(
            name="geoconnex_features",
            con=engine,
            if_exists=if_exists,
            # do not add the pandas index as a separate column
            index=False,
            schema=None,
            # write in chunks so we don't run out of memory
            chunksize=config.postgis_chunksize,
        )
        if_exists = "append"

    # new count in postgis
    with engine.begin() as conn:
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
