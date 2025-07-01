# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


from dagster import (
    AssetExecutionContext,
    asset,
)

from userCode.harvest_pipeline import harvest_sitemap
from userCode.lib.containers import (
    SynchronizerConfig,
    SynchronizerContainer,
)
from userCode.lib.dagster import sources_partitions_def
from userCode.lib.env import (
    DATAGRAPH_REPOSITORY,
    PROVGRAPH_REPOSITORY,
)

"""
All assets in the sync pipeline synchronize against a live
graphdb database
"""

SYNC_GROUP = "sync"


@asset(
    partitions_def=sources_partitions_def, deps=[harvest_sitemap], group_name=SYNC_GROUP
)
def nabu_sync(context: AssetExecutionContext, config: SynchronizerConfig):
    """Synchronize the graph with s3 by adding/removing from the graph"""
    SynchronizerContainer("sync", context.partition_key).run(
        f"sync --prefix summoned/{context.partition_key} --repository {DATAGRAPH_REPOSITORY}",
        config,
    )


@asset(
    partitions_def=sources_partitions_def, deps=[harvest_sitemap], group_name=SYNC_GROUP
)
def nabu_prov_release(context: AssetExecutionContext, config: SynchronizerConfig):
    """Construct an nq file from all of the jsonld prov produced by harvesting the sitemap.
    Used for tracing data lineage"""
    SynchronizerContainer("prov-release", context.partition_key).run(
        f"release --prefix prov/{context.partition_key}", config
    )


@asset(
    partitions_def=sources_partitions_def,
    deps=[nabu_prov_release],
    group_name=SYNC_GROUP,
)
def nabu_prov_object(context: AssetExecutionContext, config: SynchronizerConfig):
    """Take the nq file from s3 and use the sparql API to upload it into the prov graph repository"""
    SynchronizerContainer("prov-object", context.partition_key).run(
        f"object graphs/latest/{context.partition_key}_prov.nq --repository {PROVGRAPH_REPOSITORY}",
        config,
    )


@asset(
    partitions_def=sources_partitions_def, deps=[harvest_sitemap], group_name=SYNC_GROUP
)
def nabu_orgs_release(context: AssetExecutionContext, config: SynchronizerConfig):
    """Construct an nq file for the metadata of an organization. The metadata about their water data is not included in this step.
    This is just flat metadata"""
    SynchronizerContainer("orgs-release", context.partition_key).run(
        f"release --prefix orgs/{context.partition_key} --repository {DATAGRAPH_REPOSITORY}",
        config,
    )


@asset(
    partitions_def=sources_partitions_def,
    deps=[nabu_orgs_release],
    group_name=SYNC_GROUP,
)
def nabu_orgs_upload(context: AssetExecutionContext, config: SynchronizerConfig):
    """Move the orgs nq file(s) into the graphdb"""
    SynchronizerContainer("orgs", context.partition_key).run(
        f"upload --prefix orgs/{context.partition_key} --repository {DATAGRAPH_REPOSITORY}",
        config,
    )


@asset(
    partitions_def=sources_partitions_def,
    deps=[nabu_orgs_upload, nabu_sync],
    group_name=SYNC_GROUP,
)
def finished_individual_crawl(context: AssetExecutionContext):
    """Dummy asset signifying the geoconnex crawl is completed once the orgs and prov nq files are in the graphdb and the graph is synced with the s3 bucket"""
    pass
