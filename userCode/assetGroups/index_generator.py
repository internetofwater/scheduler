# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
import os
import subprocess

from dagster import (
    asset,
    get_dagster_logger,
)

from userCode.assetGroups.release_graph_generator import (
    release_graphs_for_all_summoned_jsonld,
)
from userCode.lib.containers import (
    SynchronizerConfig,
    SynchronizerContainer,
)
from userCode.lib.env import (
    ASSETS_DIRECTORY,
    GHCR_TOKEN,
    RUNNING_AS_TEST_OR_DEV,
)

"""
All assets in this pipeline work to build an index for 
qlever
"""

INDEX_GEN_GROUP = "index"

PULLED_NQ_DESTINATION = ASSETS_DIRECTORY / "geoconnex_graph/"

INDEX_DIRECTORY = ASSETS_DIRECTORY / "geoconnex_index"


@asset(
    deps=[release_graphs_for_all_summoned_jsonld],
    # this is put in a separate group since it is potentially expensive
    # and thus we don't want to run it automatically
    group_name=INDEX_GEN_GROUP,
)
def pull_release_nq_for_all_sources(config: SynchronizerConfig):
    """pull all release graphs on disk and put them in one folder"""
    PULLED_NQ_DESTINATION.mkdir(exist_ok=True)

    assert PULLED_NQ_DESTINATION.is_dir(), (
        "You must use a directory for geoconnex_graph, not a file"
    )

    fullGraphNqInContainer = "/app/geoconnex_graph/"
    volumeMapping = [f"{PULLED_NQ_DESTINATION}:{fullGraphNqInContainer}"]
    get_dagster_logger().info(
        f"Pulling release graphs to {PULLED_NQ_DESTINATION.absolute()} in host filesystem using volume mapping: {volumeMapping}"
    )
    SynchronizerContainer(
        "pull",
        "all",
        volume_mapping=volumeMapping,
    ).run(
        f"pull --prefix graphs/latest/ {fullGraphNqInContainer}",
        config,
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

    result = subprocess.run(
        qlever_cmd,
        capture_output=True,
        text=True,  # Automatically decodes output
    )

    def log(input: str):
        for line in input.splitlines():
            if line.strip() == "":  # Skip empty lines
                continue
            if "WARN" in line:
                logger.warning(line)
            else:
                logger.info(line)

    log(result.stdout)
    # qlever logs some things that aren't errors to stderr so we need to log them as normal
    log(result.stderr)

    result.check_returncode()
    get_dagster_logger().info("qlever index generation complete")

    INDEX_DIRECTORY.mkdir(exist_ok=True)

    # move all geoconnex.* files to geoconnex_index directory for cleanliness
    for path in PULLED_NQ_DESTINATION.iterdir():
        if path.is_file() and path.name.startswith("geoconnex."):
            path.rename(INDEX_DIRECTORY / path.name)


@asset(
    deps=[qlever_index],
    # this is put in a separate group since it is potentially expensive
    # and thus we don't want to run it automatically
    group_name=INDEX_GEN_GROUP,
)
def oci_artifact():
    # change directory to where the index is;
    # this allows us to use a direct path to the index;
    # oras does not interact well with full paths so it is easier to do this
    index_dir = os.path.join(os.path.dirname(__file__), "qlever")
    os.chdir(index_dir)

    date_str = datetime.now().strftime("%Y_%m_%d")

    # by putting both tags, the latest image will be labeled as such
    # but it will also have a date tag so that when a new image is
    # pushed we can refer to it by the date. tags in oci registries are essentially
    # just pointers
    tags = f"{date_str},latest"

    registry = "localhost:5000" if RUNNING_AS_TEST_OR_DEV() else "ghcr.io"

    # push the index to the remote
    filesToUpload = ""
    for file in os.listdir("geoconnex_graph"):
        if file.endswith(".nq"):
            filesToUpload += f" geoconnex_graph/{file}:application/n-quads"

    command = f"oras push {registry}/internetofwater/geoconnex-graph:{tags} {filesToUpload} --username internetofwater --password-stdin"
    get_dagster_logger().info(f"Running '{command}'")

    result = subprocess.run(
        command.split(),
        capture_output=True,
        text=True,  # Automatically decodes output
        # we provide the ghcr token here, but in localhost if we are testing against a local registry
        # this is ignored so its fine either way
        input=GHCR_TOKEN,
    )

    def log(input: str):
        for line in input.splitlines():
            if line.strip() == "":  # Skip empty lines
                continue
            if "WARN" in line:
                get_dagster_logger().warning(line)
            else:
                get_dagster_logger().info(line)

    log(result.stdout)
    # qlever logs some things that aren't errors to stderr so we need to log them as normal
    log(result.stderr)

    result.check_returncode()
    get_dagster_logger().info("Pushing qlever index to registry")

    # restore the dir
    os.chdir(os.path.dirname(__file__))
