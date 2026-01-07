# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
import os
from pathlib import Path
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
from userCode.lib.env import GHCR_TOKEN, RUNNING_AS_TEST_OR_DEV

"""
All assets in this pipeline work to build an index for 
qlever
"""

INDEX_GEN_GROUP = "index"

PULLED_NQ_DESTINATION = Path(__file__).parent / "qlever" / "geoconnex_graph/"


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
        "You must use a directory for geoconnex_graph not a file"
    )

    fullGraphNqInContainer = "/app/geoconnex_graph/"
    SynchronizerContainer(
        "pull",
        "all",
        volume_mapping=[f"{PULLED_NQ_DESTINATION}:{fullGraphNqInContainer}"],
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
    current_dir = Path(__file__).parent
    qlever_dir = current_dir / "qlever"
    qlever_cmd = [
        "qlever",
        "index",
        "--overwrite-existing",
        "--image",
        "docker.io/adfreiburg/qlever:commit-55c05d4",
    ]

    os.chdir(qlever_dir)
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
