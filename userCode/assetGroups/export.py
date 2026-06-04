# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


from datetime import datetime
import os
import subprocess

from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    Config,
    asset,
    get_dagster_logger,
)
import docker
import geopandas as gpd
import geoparquet_io as gpio
import requests
from sqlalchemy import text

from userCode.assetGroups.release_graph_generator import (
    release_graphs_for_all_summoned_jsonld,
)
from userCode.lib.classes import RcloneClient, S3
from userCode.lib.containers import (
    NqConfig,
    NqOperationsContainer,
)
from userCode.lib.dagster import (
    all_dependencies_materialized,
    dagster_log_with_parsed_level,
)
from userCode.lib.env import (
    ASSETS_DIRECTORY,
    GEOCONNEX_GRAPH_DIRECTORY,
    GEOCONNEX_INDEX_DIRECTORY,
    GHCR_TOKEN,
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


RELEASE_GRAPH_LOCATION_IN_S3 = "graphs/latest/"

DEVELOPMENT_BRANCH_IN_LAKEFS = "develop"


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


@asset(group_name=EXPORT_GROUP)
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


class ParquetConfig(Config):
    # the default location of the geoparquet is in the assets directory
    # but this can be override for testing purposes
    geoparquet_path: str = f"{ASSETS_DIRECTORY}/geoconnex_features.parquet"


@asset(
    deps=[release_graphs_for_all_summoned_jsonld],
    # this is put in a separate group since it is potentially expensive
    # and thus we don't want to run it automatically
    group_name=EXPORT_GROUP,
)
def pull_release_nq_for_all_sources(config: NqConfig):
    """pull all release graphs on disk and put them in one folder"""
    GEOCONNEX_GRAPH_DIRECTORY.mkdir(exist_ok=True)

    assert GEOCONNEX_GRAPH_DIRECTORY.is_dir(), (
        "You must use a directory for geoconnex_graph, not a file"
    )

    fullGraphNqInContainer = "/app/geoconnex_graph/"
    volumeMapping = [f"{GEOCONNEX_GRAPH_DIRECTORY}:{fullGraphNqInContainer}"]
    get_dagster_logger().info(
        f"Pulling release graphs to {GEOCONNEX_GRAPH_DIRECTORY.absolute()} in host filesystem using volume mapping: {volumeMapping}"
    )
    all_partitions = None
    NqOperationsContainer(
        partition=all_partitions,
        volume_mapping=volumeMapping,
    ).run(
        f"pull --prefix graphs/latest/ {fullGraphNqInContainer}",
        config,
    )


@asset(deps=[pull_release_nq_for_all_sources], group_name=EXPORT_GROUP)
def geoparquet_from_triples():
    client = docker.DockerClient()

    geoparquet_converter = "internetofwater/triples_to_geoparquet:latest"
    get_dagster_logger().info(f"Pulling {geoparquet_converter}")
    client.images.pull(geoparquet_converter)
    get_dagster_logger().info(
        f"Running triples_to_geoparquet on {GEOCONNEX_GRAPH_DIRECTORY.absolute()}"
    )

    container = client.containers.run(
        image=geoparquet_converter,
        name="triples_to_geoparquet",
        detach=True,
        command="--triples /app/assets/geoconnex_graph --output /app/assets/geoconnex_features.parquet",
        volumes=[
            f"{ASSETS_DIRECTORY.absolute()}:/app/assets/",
        ],
        auto_remove=True,
    )

    for line in container.logs(stdout=True, stderr=True, stream=True, follow=True):
        decoded = line.decode("utf-8")
        dagster_log_with_parsed_level(decoded)

    exit_status: int = container.wait()["StatusCode"]
    get_dagster_logger().info(f"Container Wait Exit status:  {exit_status}")

    if exit_status != 0:
        raise Exception(f"triples_to_geoparquet failed with exit status {exit_status}")

    geoparquet_file = ASSETS_DIRECTORY / "geoconnex_features.parquet"

    (
        gpio.read(geoparquet_file)
        .add_bbox()
        .sort_hilbert()
        .add_bbox_metadata()
        .write(geoparquet_file, overwrite=True, row_group_size_mb=4)
    )

    result = gpio.read(geoparquet_file).check()
    if result.passed():
        get_dagster_logger().info(
            "Geoparquet passed all checks and appears to be valid"
        )
    else:
        get_dagster_logger().warning("Geoparquet has issues:")
        for res in result.failures():
            get_dagster_logger().warning(res)

    assert geoparquet_file.is_file(), (
        f"{geoparquet_file} is not a file and thus cannot be uploaded"
    )

    if RUNNING_AS_TEST_OR_DEV():
        get_dagster_logger().warning("Skipping export as we are running in test mode")
        return

    s3 = S3(bucket="metadata-geoconnex-us")

    get_dagster_logger().info(
        f"Uploading {geoparquet_file.name} of size {geoparquet_file.stat().st_size} to bucket '{s3.bucket}' in the object store"
    )
    with geoparquet_file.open("rb") as f:
        s3.load_stream(
            stream=f,
            remote_path=f"exports/{geoparquet_file.name}",
            content_length=geoparquet_file.stat().st_size,
            content_type="application/vnd.apache.parquet",
            headers={},
        )


@asset(
    deps=[pull_release_nq_for_all_sources],
    # this is put in a separate group since it is potentially expensive
    # and thus we don't want to run it automatically
    group_name=EXPORT_GROUP,
)
def qlever_index():
    logger = get_dagster_logger()

    os.chdir(ASSETS_DIRECTORY)

    # overwrite existing index if it exists and use up to 11GB of memory for building the
    # index; otherwise qlever will only use the default of 1GB
    qlever_cmd = ["qlever", "index", "--overwrite-existing", "--stxxl-memory", "11GB"]

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


@asset(
    deps=[pull_release_nq_for_all_sources],
    # this is put in a separate group since it is potentially expensive
    # and thus we don't want to run it automatically
    group_name=EXPORT_GROUP,
)
def oci_artifact():
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
    deps=[geoparquet_from_triples],
)
def move_geoparquet_to_postgis(config: ParquetConfig):
    """
    Load geoparquet and write it into PostGIS using GeoPandas to_postgis.
    """

    engine = new_sqlalchemy_engine_from_env()

    get_dagster_logger().info(
        f"Moving geoparquet data from file {config.geoparquet_path} to PostGIS"
    )
    # Read geoparquet
    gdf = gpd.read_parquet(config.geoparquet_path)

    gdf.set_crs(epsg=4326, inplace=True, allow_override=True)

    # Write to PostGIS
    gdf.to_postgis(
        name="geoconnex_features",
        con=engine,
        if_exists="replace",
        # do not add the pandas index as a separate column
        index=False,
        schema=None,
        # write 100k rows at a time so
        # we don't run out of memory
        chunksize=100_000,
    )

    # new count in postgis
    with engine.begin() as conn:
        result = conn.execute(text("SELECT count(*) FROM geoconnex_features;"))
        count = result.scalar()

        get_dagster_logger().info("Creating indexes on geoconnex_features table")
        conn.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_geoconnex_features_id
            ON geoconnex_features (id);
        """)
        )

        conn.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_geoconnex_sitemap
            ON geoconnex_features (geoconnex_sitemap);
            """)
        )
    get_dagster_logger().info(
        f"Finishing moving Parquet data into postgis. Table 'geoconnex_features' now has {count} rows."
    )


@asset(
    group_name=EXPORT_GROUP,
    deps=[pull_release_nq_for_all_sources],
)
def stream_all_release_graphs_to_renci(
    context: AssetExecutionContext,
    rclone_config: str,
):
    """
    Stream all release graphs to RENCI
    """
    if RUNNING_AS_TEST_OR_DEV():
        get_dagster_logger().warning("Skipping export as we are running in test mode")
        return
    lakefs_client = LakeFSClient("geoconnex")
    get_dagster_logger().info(
        f"Uploading release graphs from {RELEASE_GRAPH_LOCATION_IN_S3} to lakefs at {DEVELOPMENT_BRANCH_IN_LAKEFS}"
    )
    RcloneClient(rclone_config).copy_directory_to_lakefs(
        destination_branch=DEVELOPMENT_BRANCH_IN_LAKEFS,
        source_prefix=RELEASE_GRAPH_LOCATION_IN_S3,
        lakefs_client=lakefs_client,
    )


@asset(group_name=EXPORT_GROUP, deps=[pull_release_nq_for_all_sources])
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
            f"{GEOCONNEX_GRAPH_DIRECTORY} does not exist and thus the release graphs cannot be uploaded"
        )

    # Read file stream from local release graph
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
