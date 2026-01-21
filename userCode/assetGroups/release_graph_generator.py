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
    MAINSTEM_CONTAINER_FILE_MOUNT,
    SynchronizerConfig,
    SynchronizerContainer,
)
from userCode.lib.dagster import sources_partitions_def
from userCode.lib.env import MAINSTEM_FILE

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

    def verify_valid(path: Path):
        if not path.exists():
            raise ValueError(f"Mainstem file {path} does not exist")
        if not path.is_file():
            raise ValueError(f"Mainstem file {path} is not a file")

    if override_mainstem_file:
        verify_valid(Path(override_mainstem_file))
        mainstem_file = override_mainstem_file
    else:
        verify_valid(MAINSTEM_FILE)
        mainstem_file = str(MAINSTEM_FILE.absolute())

    volume_mapping: list | None = (
        [f"{mainstem_file}:{MAINSTEM_CONTAINER_FILE_MOUNT}"] if mainstem_file else None
    )

    if volume_mapping:
        get_dagster_logger().info(f"Mainstem file volume mapping: {mainstem_file}")

    SynchronizerContainer(
        "release",
        context.partition_key,
        volume_mapping=volume_mapping,
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
