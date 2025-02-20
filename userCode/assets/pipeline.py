# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from dagster import (
    AssetExecutionContext,
    asset,
)
from userCode.assets.configs import (
    docker_client_environment,
    gleaner_config,
    nabu_config,
)
from userCode.lib.containers import GleanerContainer, NabuContainer
from userCode.lib.env import (
    GLEANERIO_DATAGRAPH_ENDPOINT,
    GLEANERIO_PROVGRAPH_ENDPOINT,
)
from userCode.lib.dagster import sources_partitions_def

"""
This file defines all of the core assets that make up the
Geoconnex pipeline. It does not deal with external exports
or dagster sensors that trigger it, just the core pipeline
"""


@asset(
    partitions_def=sources_partitions_def,
    deps=[docker_client_environment, gleaner_config, nabu_config],
)
def gleaner(context: AssetExecutionContext):
    """Get the jsonld for each site in the gleaner config"""
    GleanerContainer(context.partition_key).run(
        ["--cfg", "gleanerconfig.yaml", "--source", context.partition_key, "--rude"]
    )


@asset(partitions_def=sources_partitions_def, deps=[gleaner])
def nabu_release(context: AssetExecutionContext):
    """Construct an nq file from all of the jsonld produced by gleaner"""
    NabuContainer("release", context.partition_key).run(
        [
            "release",
            "--cfg",
            "nabuconfig.yaml",
            "--prefix",
            "summoned/" + context.partition_key,
        ]
    )


@asset(partitions_def=sources_partitions_def, deps=[nabu_release])
def nabu_object(context: AssetExecutionContext):
    """Take the nq file from s3 and use the sparql API to upload it into the graph"""
    NabuContainer("object", context.partition_key).run(
        [
            "--cfg",
            "nabuconfig.yaml",
            "object",
            f"graphs/latest/{context.partition_key}_release.nq",
            "--repository",
            GLEANERIO_DATAGRAPH_ENDPOINT,
        ]
    )


@asset(partitions_def=sources_partitions_def, deps=[nabu_object])
def nabu_prune(context: AssetExecutionContext):
    """Synchronize the graph with s3 by adding/removing from the graph"""
    NabuContainer("prune", context.partition_key).run(
        [
            "--cfg",
            "nabuconfig.yaml",
            "prune",
            "--prefix",
            "summoned/" + context.partition_key,
            "--repository",
            GLEANERIO_DATAGRAPH_ENDPOINT,
        ]
    )


@asset(partitions_def=sources_partitions_def, deps=[gleaner])
def nabu_prov_release(context: AssetExecutionContext):
    """Construct an nq file from all of the jsonld prov produced by gleaner.
    Used for tracing data lineage"""
    NabuContainer("prov-release", context.partition_key).run(
        [
            "--cfg",
            "nabuconfig.yaml",
            "release",
            "--prefix",
            "prov/" + context.partition_key,
        ]
    )


@asset(partitions_def=sources_partitions_def, deps=[gleaner])
def nabu_prov_clear(context: AssetExecutionContext):
    """Clears the prov graph before putting the new nq in"""
    NabuContainer("prov-clear", context.partition_key).run(
        [
            "--cfg",
            "nabuconfig.yaml",
            "clear",
            "--repository",
            GLEANERIO_PROVGRAPH_ENDPOINT,
        ]
    )


@asset(partitions_def=sources_partitions_def, deps=[nabu_prov_clear, nabu_prov_release])
def nabu_prov_object(context: AssetExecutionContext):
    """Take the nq file from s3 and use the sparql API to upload it into the prov graph repository"""
    NabuContainer("prov-object", context.partition_key).run(
        [
            "--cfg",
            "nabuconfig.yaml",
            "object",
            f"graphs/latest/{context.partition_key}_prov.nq",
            "--repository",
            GLEANERIO_PROVGRAPH_ENDPOINT,
        ]
    )


@asset(partitions_def=sources_partitions_def, deps=[gleaner])
def nabu_orgs_release(context: AssetExecutionContext):
    """Construct an nq file for the metadata of all the organizations. Their data is not included in this step.
    This is just flat metadata"""
    NabuContainer("orgs-release", context.partition_key).run(
        [
            "--cfg",
            "nabuconfig.yaml",
            "release",
            "--prefix",
            "orgs/",
            "--repository",
            GLEANERIO_DATAGRAPH_ENDPOINT,
        ]
    )


@asset(partitions_def=sources_partitions_def, deps=[nabu_orgs_release])
def nabu_orgs_prefix(context: AssetExecutionContext):
    """Move the orgs nq file(s) into the graphdb"""
    NabuContainer("orgs", context.partition_key).run(
        [
            "--cfg",
            "nabuconfig.yaml",
            "prefix",
            "--prefix",
            "orgs",
            "--repository",
            GLEANERIO_DATAGRAPH_ENDPOINT,
        ]
    )


@asset(
    partitions_def=sources_partitions_def,
    deps=[nabu_orgs_prefix, nabu_prune],
)
def finished_individual_crawl(context: AssetExecutionContext):
    """Dummy asset signifying the geoconnex crawl is completed once the orgs and prov nq files are in the graphdb and the graph is synced with the s3 bucket"""
    pass
