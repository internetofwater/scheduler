# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


from dagster import (
    AssetExecutionContext,
    asset,
)

from userCode.assetGroups.config import docker_client_environment, sitemap_partitions
from userCode.lib.containers import (
    SitemapHarvestConfig,
    SitemapHarvestContainer,
)
from userCode.lib.dagster import AllPartitions

"""
This file contains all assets relevant to crawling / harvesting
remote data from a sitemap
"""

HARVEST_GROUP = "harvest"


# a tag representing whether we should exit 3 on failure
# this is used in the harvest_sitemap asset to
EXIT_3_IS_FATAL = "exit_3_is_fatal"


@asset(
    partitions_def=AllPartitions.generic_partition_def,
    deps=[docker_client_environment, sitemap_partitions],
    group_name=HARVEST_GROUP,
)
def harvest_non_usgs_sitemap(
    context: AssetExecutionContext, config: SitemapHarvestConfig
):
    """Harvest a sitemap not hosted by USGS"""
    harvest_sitemap(context, config)


@asset(
    partitions_def=AllPartitions.usgs_partitions_def,
    deps=[docker_client_environment, sitemap_partitions],
    group_name=HARVEST_GROUP,
    tags={"host": "usgs"},
)
def harvest_usgs_sitemap(context: AssetExecutionContext, config: SitemapHarvestConfig):
    """Harvest a sitemap hosted by USGS"""
    harvest_sitemap(context, config)


def harvest_sitemap(
    context: AssetExecutionContext,
    config: SitemapHarvestConfig,
):
    """Harvest the jsonld for each site in the sitemap"""
    if context.has_tag(EXIT_3_IS_FATAL):
        # we have to dump and reassign since pydantic classes are frozen
        old_config = config.model_dump()
        old_config[EXIT_3_IS_FATAL] = True
        strictConfig = SitemapHarvestConfig(**old_config)
        SitemapHarvestContainer(context.partition_key).run(strictConfig)
    else:
        SitemapHarvestContainer(context.partition_key).run(config)
