# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from dagster import (
    AssetExecutionContext,
    asset,
)

from userCode.assetGroups.harvest import harvest_sitemap
from userCode.lib.containers import (
    SynchronizerConfig,
    SynchronizerContainer,
)
from userCode.lib.dagster import sources_partitions_def

"""
All assets in this graph work to generate a release graph for each partition
"""

RELEASE_GRAPH_GENERATOR_GROUP = "release_graphs"


@asset(
    partitions_def=sources_partitions_def,
    deps=[harvest_sitemap],
    group_name=RELEASE_GRAPH_GENERATOR_GROUP,
)
def release_graphs_for_all_summoned_jsonld(
    context: AssetExecutionContext, config: SynchronizerConfig
):
    """Construct an nq file from all the jsonld for a single sitemap"""
    SynchronizerContainer("prov-release", context.partition_key).run(
        f"release --compress --prefix summoned/{context.partition_key}", config
    )


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
