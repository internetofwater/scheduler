# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import os
from dagster import (
    DagsterInstance,
    load_assets_from_modules,
    materialize,
)
from userCode import pipeline
from userCode.lib.classes import RcloneClient
import userCode.main as main
from userCode.main import definitions
from userCode.pipeline import sources_partitions_def

from dagster import AssetsDefinition, AssetSpec, SourceAsset

from .helpers import SparqlClient


def assert_data_is_linked_in_graph():
    """Check that a mainstem is associated with a monitoring location in the graph"""
    query = """
    select * where {
        <https://geoconnex.us/cdss/gages/FARMERCO> <https://schema.org/name> ?o .
    } limit 100
    """

    resultDict = SparqlClient().execute_sparql(query)
    assert "FLORIDA FARMERS CANAL" in resultDict["o"]

    query = """
    PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>

    select * where { 
        ?monitoringLocation hyf:referencedPosition/hyf:HY_IndirectPosition/hyf:linearElement <https://geoconnex.us/ref/mainstems/42750> .
    } limit 100 
    """
    resultDict = SparqlClient().execute_sparql(query)
    # make sure that the florida canal monitoring location is on the florida river mainstem
    assert len(resultDict["monitoringLocation"]) > 0, (
        "There were no linked monitoring locations for the Florida River Mainstem"
    )
    assert (
        "https://geoconnex.us/cdss/gages/FLOCANCO" in resultDict["monitoringLocation"]
    )


def assert_rclone_is_installed_properly():
    location = RcloneClient.get_config_path()
    assert location.parent.exists(), f"{location} does not exist"
    assert os.system("rclone version") == 0


def test_e2e():
    """Run the e2e test on the entire geoconnex graph"""
    SparqlClient().clear_graph()
    # insert a dummy graph before running that should be dropped after syncing ref_mainstems_mainstems__0
    SparqlClient().insert_triples_as_graph(
        "urn:iow:summoned:ref_mainstems_mainstems__0:DUMMY_PREFIX_TO_DROP",
        """
        <http://example.org/resource/1> <http://example.org/property/name> "Alice" .
        <http://example.org/resource/2> <http://example.org/property/name> "Bob" .
        """,
    )

    instance = DagsterInstance.ephemeral()
    assets = load_assets_from_modules([pipeline])
    # It is possible to load certain asset types that cannot be passed into
    # Materialize so we filter them to avoid a pyright type error
    filtered_assets = [
        asset
        for asset in assets
        if isinstance(asset, (AssetsDefinition, AssetSpec, SourceAsset))
    ]
    # These three assets are needed to generate the dynamic partition.
    all_graphs = materialize(
        assets=filtered_assets,
        selection=["nabu_config", "gleaner_config", "docker_client_environment"],
        instance=instance,
    )
    assert all_graphs.success

    resolved_job = definitions.get_job_def("harvest_source")

    all_partitions = sources_partitions_def.get_partition_keys(
        dynamic_partitions_store=instance
    )
    assert len(all_partitions) > 0, "Partitions were not generated"

    all_graphs = resolved_job.execute_in_process(
        instance=instance, partition_key="ref_mainstems_mainstems__0"
    )

    assert all_graphs.success, "Job execution failed for partition 'mainstems__0'"

    objects_query = """
    select * where {
        <https://geoconnex.us/ref/mainstems/42750> <https://schema.org/name> ?o .
    } limit 100
    """

    resultDict = SparqlClient().execute_sparql(objects_query)
    assert "Florida River" in resultDict["o"], (
        "The Florida River Mainstem was not found in the graph"
    )

    all_graphs = resolved_job.execute_in_process(
        instance=instance, partition_key="cdss_co_gages__0"
    )
    assert all_graphs.success, "Job execution failed for partition 'cdss_co_gages__0'"

    assert_data_is_linked_in_graph()
    # Don't want to actually transfer the file but should check it is installed
    assert_rclone_is_installed_properly()

    assert (
        definitions.get_job_def("export_nquads")
        .execute_in_process(instance=instance)
        .success
    )

    all_graphs = SparqlClient().execute_sparql("""
    SELECT DISTINCT ?g 
    WHERE { 
        GRAPH ?g { 
            ?s ?p ?o .
        } 
    }
    """)
    # make sure we have 2 orgs graphs since we crawled 2 sources so far
    # urn:iow:orgs is nabu's way of serializing the s3 prefix 'orgs/'
    assert sum("urn:iow:orgs" in g for g in all_graphs["g"]) == 2
    assert not any("DUMMY_PREFIX_TO_DROP" in g for g in all_graphs["g"]), (
        "The dummy graph we inserted crawling was not dropped correctly"
    )


def test_dynamic_partitions():
    """Make sure that a new materialization of the gleaner config will create new partitions"""
    instance = DagsterInstance.ephemeral()
    mocked_partition_keys = ["test_partition1", "test_partition2", "test_partition3"]
    instance.add_dynamic_partitions(
        partitions_def_name="sources_partitions_def",
        partition_keys=list(mocked_partition_keys),
    )

    assert (
        instance.get_dynamic_partitions("sources_partitions_def")
        == mocked_partition_keys
    )

    assets = load_assets_from_modules([main])
    # It is possible to load certain asset types that cannot be passed into
    # Materialize so we filter them to avoid a pyright type error
    filtered_assets = [
        asset
        for asset in assets
        if isinstance(asset, (AssetsDefinition, AssetSpec, SourceAsset))
    ]
    # These three assets are needed to generate the dynamic partition.
    result = materialize(
        assets=filtered_assets,
        selection=["gleaner_config"],
        instance=instance,
    )
    assert result.success, "Expected gleaner config to materialize"

    assert (
        instance.get_dynamic_partitions("sources_partitions_def")
        != mocked_partition_keys
    )
    newPartitions = instance.get_dynamic_partitions("sources_partitions_def")

    # Make sure that the old partition keys aren't in the asset but
    # the new ones are
    for key in mocked_partition_keys:
        assert key not in newPartitions
    assert "ref_mainstems_mainstems__0" in newPartitions
    assert "ref_dams_dams__0" in newPartitions

    # Check what happens when we delete a specific key in the dynamic partition
    instance.delete_dynamic_partition(
        "sources_partitions_def", "ref_mainstems_mainstems__0"
    )

    # Make sure that partitions are deleted
    partitionsAfterDelete = instance.get_dynamic_partitions("sources_partitions_def")
    assert "ref_mainstems_mainstems__0" not in partitionsAfterDelete
    assert "ref_dams_dams__0" in partitionsAfterDelete
    assert len(partitionsAfterDelete) == len(newPartitions) - 1
    for key in mocked_partition_keys:
        assert key not in partitionsAfterDelete
