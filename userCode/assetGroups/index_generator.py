# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

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
def concatenated_release_nq_for_all_sources(config: SynchronizerConfig):
    """Concatenate all release graphs on disk and form one large nq file"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    fullGraphNq = os.path.join(current_dir, "qlever", "geoconnex_graph.nq")

    # Ensure it's a file, not a directory
    assert not os.path.isdir(fullGraphNq), (
        "You must use a file for geoconnex_graph.nq, not a directory"
    )
    if not os.path.exists(fullGraphNq):
        # if the file doesn't exist then create it
        # so docker can use it for the volume mount
        with open(fullGraphNq, "w"):
            pass

    fullGraphNqInContainer = "/app/geoconnex_graph.nq"
    SynchronizerContainer(
        "concat", "all", volume_mapping=[f"{fullGraphNq}:{fullGraphNqInContainer}"]
    ).run(
        f"concat --prefix graphs/latest {fullGraphNqInContainer}",
        config,
    )


@asset(
    deps=[concatenated_release_nq_for_all_sources],
    # this is put in a separate group since it is potentially expensive
    # and thus we don't want to run it automatically
    group_name=INDEX_GEN_GROUP,
)
def qlever_index():
    logger = get_dagster_logger()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    qlever_dir = os.path.join(current_dir, "qlever")
    qlever_cmd = ["qlever", "index", "--overwrite-existing"]

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
