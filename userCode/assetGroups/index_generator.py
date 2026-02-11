# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
import os
import subprocess

from dagster import (
    asset,
    get_dagster_logger,
)
import docker
import geoparquet_io as gpio

from userCode.assetGroups.release_graph_generator import (
    release_graphs_for_all_summoned_jsonld,
)
from userCode.lib.classes import S3
from userCode.lib.containers import (
    SynchronizerConfig,
    SynchronizerContainer,
)
from userCode.lib.dagster import dagster_log_with_parsed_level
from userCode.lib.env import (
    ASSETS_DIRECTORY,
    GEOCONNEX_GRAPH_DIRECTORY,
    GEOCONNEX_INDEX_DIRECTORY,
    GHCR_TOKEN,
    RUNNING_AS_TEST_OR_DEV,
)

"""
All assets in this pipeline work to build an index for 
qlever
"""

INDEX_GEN_GROUP = "index"


@asset(
    deps=[release_graphs_for_all_summoned_jsonld],
    # this is put in a separate group since it is potentially expensive
    # and thus we don't want to run it automatically
    group_name=INDEX_GEN_GROUP,
)
def pull_release_nq_for_all_sources(config: SynchronizerConfig):
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
    SynchronizerContainer(
        "pull",
        "all",
        volume_mapping=volumeMapping,
    ).run(
        f"pull --prefix graphs/latest/ {fullGraphNqInContainer}",
        config,
    )


@asset(deps=[pull_release_nq_for_all_sources], group_name=INDEX_GEN_GROUP)
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

    s3 = S3()

    get_dagster_logger().info(
        f"Uploading {geoparquet_file.name} of size {geoparquet_file.stat().st_size} to the object store"
    )
    with geoparquet_file.open("rb") as f:
        s3.load_stream(
            stream=f,
            remote_path=f"geoconnex_exports/{geoparquet_file.name}",
            content_length=geoparquet_file.stat().st_size,
            content_type="application/vnd.apache.parquet",
            headers={},
        )


@asset(
    deps=[pull_release_nq_for_all_sources],
    # this is put in a separate group since it is potentially expensive
    # and thus we don't want to run it automatically
    group_name=INDEX_GEN_GROUP,
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
    deps=[qlever_index],
    # this is put in a separate group since it is potentially expensive
    # and thus we don't want to run it automatically
    group_name=INDEX_GEN_GROUP,
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

    command = f"oras push {registry}/internetofwater/geoconnex-graph:{tags} {' '.join(files_to_upload)} --username internetofwater --password-stdin"

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
