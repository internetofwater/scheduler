# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


from pathlib import Path

from dagster import (
    AssetExecutionContext,
    asset,
    get_dagster_logger,
)

from userCode.assetGroups.config import (
    docker_client_environment,
    mainstem_catchment_metadata,
    sitemap_partitions,
)
from userCode.lib.containers import (
    SitemapHarvestConfig,
    SitemapHarvestContainer,
)
from userCode.lib.dagster import sources_partitions_def
from userCode.lib.env import MAINSTEM_FILE, RUNNING_AS_TEST_OR_DEV

"""
This file contains all assets relevant to crawling / harvesting
remote data from a sitemap into one parquet file per sitemap in s3
"""

HARVEST_GROUP = "harvest"


# a tag representing whether we should exit 3 on failure
# this is used in the harvest_sitemap asset to
EXIT_3_IS_FATAL = "exit_3_is_fatal"
# tag exclusively used for testing and overriding the default mainstem
# file set by the .env file
MAINSTEM_FILE_OVERRIDE_TAG = "mainstem_file_override"


def verify_mainstem_file(file: Path):
    if not file.exists():
        raise Exception(f"Mainstem file {file} does not exist")
    if not file.is_file():
        raise Exception(f"Mainstem file {file} is not a file")


def mainstem_file_and_volume_mount(
    context: AssetExecutionContext,
) -> tuple[str, list[str]]:
    """Get the location of the mainstem file that nabu should use and any volume mount needed to access it"""
    override_mainstem_file = context.get_tag(MAINSTEM_FILE_OVERRIDE_TAG)

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
        return mainstem_file, volume_mount

    verify_mainstem_file(MAINSTEM_FILE)
    # nabu runs in the docker network so it refers to the
    # asset server by its docker network name
    mainstem_file = f"http://asset_server:80/{MAINSTEM_FILE.name}"
    get_dagster_logger().info(
        f"Using mainstem file '{mainstem_file}' for adding mainstems to harvested features"
    )
    return mainstem_file, []


@asset(
    partitions_def=sources_partitions_def,
    deps=[docker_client_environment, sitemap_partitions, mainstem_catchment_metadata],
    group_name=HARVEST_GROUP,
    pool="harvest_pool",
)
def harvest_sitemap(
    context: AssetExecutionContext,
    config: SitemapHarvestConfig,
):
    """Harvest the jsonld for each site in the sitemap into a parquet file in s3"""
    mainstem_file, volume_mount = mainstem_file_and_volume_mount(context)
    container = SitemapHarvestContainer(
        context.partition_key,
        mainstem_file=mainstem_file,
        volume_mapping=volume_mount,
    )
    if context.has_tag(EXIT_3_IS_FATAL):
        # we have to dump and reassign since pydantic classes are frozen
        old_config = config.model_dump()
        old_config[EXIT_3_IS_FATAL] = True
        strictConfig = SitemapHarvestConfig(**old_config)
        container.run(strictConfig)
    else:
        container.run(config)
