# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


from pathlib import Path

from dagster import (
    AssetExecutionContext,
    asset,
    get_dagster_logger,
)

from userCode.assetGroups.harvest import harvest_sitemap
from userCode.lib.containers import (
    SynchronizerConfig,
    SynchronizerContainer,
)
from userCode.lib.dagster import sources_partitions_def
from userCode.lib.env import MAINSTEM_FILE, RUNNING_AS_TEST_OR_DEV

"""
All assets in this graph work to generate a release graph for each partition
"""

RELEASE_GRAPH_GENERATOR_GROUP = "release_graphs"
ADD_MAINSTEM_INFO_TAG = "add_mainstem_info"
# tag exclusively used for testing and overriding the default mainstem
# file set by the .env file
MAINSTEM_FILE_OVERRIDE_TAG = "mainstem_file_override"


@asset(
    partitions_def=sources_partitions_def,
    deps=[harvest_sitemap],
    group_name=RELEASE_GRAPH_GENERATOR_GROUP,
)
def release_graphs_for_all_summoned_jsonld(
    context: AssetExecutionContext, config: SynchronizerConfig
):
    """Construct an nq file from all the jsonld for a single sitemap"""

    if not context.get_tag(ADD_MAINSTEM_INFO_TAG):
        get_dagster_logger().warning(
            f"The tag '{ADD_MAINSTEM_INFO_TAG}' was not set; skipping adding the mainstem metadata file to the release graphs"
        )
    else:
        get_dagster_logger().info(
            f"The tag '{ADD_MAINSTEM_INFO_TAG}' was set; adding the mainstem metadata file to the release graphs"
        )

    override_mainstem_file = context.get_tag(MAINSTEM_FILE_OVERRIDE_TAG)

    def verify_mainstem_file(file: Path):
        if not file.exists():
            raise Exception(f"Mainstem file {file} does not exist")
        if not file.is_file():
            raise Exception(f"Mainstem file {file} is not a file")

    if override_mainstem_file:
        mainstem_file = Path(override_mainstem_file)
    else:
        mainstem_file = MAINSTEM_FILE

    verify_mainstem_file(mainstem_file)
    if not RUNNING_AS_TEST_OR_DEV:
        mainstem_file = f"http://asset_server:8089/{mainstem_file.name}"
    else:
        mainstem_file = f"http://localhost:8089/{mainstem_file.name}"

    SynchronizerContainer(
        "release",
        context.partition_key,
        mainstem_file=mainstem_file,
    ).run(f"release --compress --prefix summoned/{context.partition_key}", config)


@asset(
    partitions_def=sources_partitions_def,
    deps=[harvest_sitemap],
    group_name=RELEASE_GRAPH_GENERATOR_GROUP,
)
def release_graphs_for_org_metadata(
    context: AssetExecutionContext, config: SynchronizerConfig
):
    """Construct an nq file for the metadata of an organization."""
    SynchronizerContainer("orgs-release", context.partition_key).run(
        f"release --compress --prefix orgs/{context.partition_key}",
        config,
    )
