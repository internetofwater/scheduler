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
        mainstem_file = None
        volume_mount = None
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

        # if the user specified that they want to override the mainstem file
        # and provide a custom one (usually for testing) try to mount it in
        # this only works in dev since docker doesn't permit mounting files in a
        # container launched by another container
        if override_mainstem_file:
            if not RUNNING_AS_TEST_OR_DEV():
                raise Exception(
                    f"The tag '{MAINSTEM_FILE_OVERRIDE_TAG}' was set to provide a custom local override file of '{override_mainstem_file}' \
                        but this is not a test or dev environment so it is impossible to mount a file with a container launched by another container"
                )
            override_path = Path(override_mainstem_file)
            verify_mainstem_file(override_path)
            mainstem_file = f"/app/{override_path.name}"
            volume_mount = [f"{override_mainstem_file}:{mainstem_file}"]
            get_dagster_logger().info(f"Mounting mainstem file with {volume_mount}")
        else:
            mainstem_file = MAINSTEM_FILE
            volume_mount = None
            verify_mainstem_file(mainstem_file)
            if not RUNNING_AS_TEST_OR_DEV:
                mainstem_file = f"http://localhost:8089/{mainstem_file.name}"
            else:
                # if we are running in prod we assume we are in the docker network and thus
                # refer to the service by its docker network name
                mainstem_file = f"http://asset_server:80/{mainstem_file.name}"

            get_dagster_logger().info(
                f"Using mainstem file '{mainstem_file}' for adding extra metadata to release graphs"
            )

    SynchronizerContainer(
        "release",
        context.partition_key,
        mainstem_file=mainstem_file,
        volume_mapping=volume_mount,
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
