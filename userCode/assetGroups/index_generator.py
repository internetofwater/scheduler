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
