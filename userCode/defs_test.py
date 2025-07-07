# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from dagster import (
    AssetSpec,
    AssetsDefinition,
    DagsterInstance,
    SourceAsset,
    load_assets_from_modules,
    materialize,
)

from test.lib import SparqlClient, assert_rclone_config_is_accessible
from userCode.assetGroups import config, harvest
from userCode.assetGroups.harvest import (
    EXIT_3_IS_FATAL,
    sources_partitions_def,
)
import userCode.defs as defs


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


def test_e2e():
    """Run the e2e test on the entire geoconnex graph"""
    SparqlClient("iow").clear_graph()
    SparqlClient("iowprov").clear_graph()
    # insert a dummy graph before running that should be dropped after syncing ref_mainstems_mainstems__0
    SparqlClient().insert_triples_as_graph(
        "urn:iow:summoned:ref_mainstems_mainstems__0:DUMMY_PREFIX_TO_DROP",
        """
        <http://example.org/resource/1> <http://example.org/property/name> "Alice" .
        <http://example.org/resource/2> <http://example.org/property/name> "Bob" .
        """,
    )

    instance = DagsterInstance.ephemeral()
    assets = load_assets_from_modules([harvest, config])
    # It is possible to load certain asset types that cannot be passed into
    # Materialize so we filter them to avoid a pyright type error
    filtered_assets = [
        asset
        for asset in assets
        if isinstance(asset, AssetsDefinition | AssetSpec | SourceAsset)
    ]
    # These three assets are needed to generate the dynamic partition.
    all_graphs = materialize(
        assets=filtered_assets,
        selection=["sitemap_partitions", "docker_client_environment", "rclone_config"],
        instance=instance,
    )
    assert all_graphs.success

    all_partitions = sources_partitions_def.get_partition_keys(
        dynamic_partitions_store=instance
    )
    assert len(all_partitions) > 0, "Partitions were not generated"

    harvest_and_sync_job = defs.defs.get_job_def("harvest_and_sync")

    assert harvest_and_sync_job.execute_in_process(
        instance=instance,
        tags={EXIT_3_IS_FATAL: str(True)},
        partition_key="ref_mainstems_mainstems__0",
    ).success, "Job execution failed for partition 'mainstems__0'"

    objects_query = """
    select * where {
        <https://geoconnex.us/ref/mainstems/42750> <https://schema.org/name> ?o .
    } limit 100
    """

    resultDict = SparqlClient().execute_sparql(objects_query)
    assert "Florida River" in resultDict["o"], (
        "The Florida River Mainstem was not found in the graph"
    )

    assert harvest_and_sync_job.execute_in_process(
        instance=instance, partition_key="cdss_co_gages__0"
    ).success, "Job execution failed for partition 'cdss_co_gages__0'"

    assert_data_is_linked_in_graph()
    # Don't want to actually transfer the file but should check it is installed
    assert_rclone_config_is_accessible()

    assert (
        defs.defs.get_job_def("export_nquads")
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
    NUM_ORG_GRAPHS = sum("urn:iow:orgs" in g for g in all_graphs["g"])
    assert NUM_ORG_GRAPHS == 2
    assert not any("DUMMY_PREFIX_TO_DROP" in g for g in all_graphs["g"]), (
        "The dummy graph we inserted crawling was not dropped correctly"
    )

    # make sure that prov graphs were generated for the mainstem run
    mainstem_prov_graphs = SparqlClient(repository="iowprov").execute_sparql("""
        SELECT DISTINCT ?g
        WHERE {
        GRAPH ?g {
            ?s ?p ?o .
        }
        FILTER(CONTAINS(STR(?g), "urn:iow:prov:ref_mainstems_mainstems__0"))
        }
        """)
    assert len(mainstem_prov_graphs["g"]) > 0, (
        "prov graphs were not generated for the mainstem run"
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

    assets = load_assets_from_modules([defs])
    # It is possible to load certain asset types that cannot be passed into
    # Materialize so we filter them to avoid a pyright type error
    filtered_assets = [
        asset
        for asset in assets
        if isinstance(asset, AssetsDefinition | AssetSpec | SourceAsset)
    ]
    # These three assets are needed to generate the dynamic partition.
    result = materialize(
        assets=filtered_assets,
        selection=["sitemap_partitions"],
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
