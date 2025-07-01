# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import os

from dagster import (
    asset,
)

from userCode.assetGroups.release_graph_generator import (
    release_graphs_for_all_summoned_jsonld,
)
from userCode.lib.containers import (
    SynchronizerConfig,
    SynchronizerContainer,
)
from userCode.lib.env import repositoryRoot

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
    indexnq = os.path.join(repositoryRoot, "index.nq")
    # Ensure it's a file, not a directory
    assert not os.path.isdir(indexnq), (
        "You must use a file for index.nq, not a directory"
    )
    if not os.path.exists(indexnq):
        # if the file doesn't exist then create it
        # so docker can use it for the volume mount
        with open(indexnq, "w"):
            pass

    indexnqPathInContainer = "/app/index.nq"
    SynchronizerContainer(
        "concat", "all", volume_mapping=[f"{indexnq}:{indexnqPathInContainer}"]
    ).run(
        f"concat --prefix graphs/latest {indexnqPathInContainer}",
        config,
    )
