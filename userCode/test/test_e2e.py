from dagster import (
    DagsterInstance,
    load_assets_from_modules,
    materialize,
)
import userCode.main as main
from userCode.main import definitions, sources_partitions_def


from dagster import AssetsDefinition, AssetSpec, SourceAsset

from userCode.test.lib import execute_sparql


def test_materialize_ref_hu02():
    instance = DagsterInstance.ephemeral()
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
        selection=["nabu_config", "gleaner_config", "docker_client_environment"],
        instance=instance,
    )
    assert result.success

    resolved_job = definitions.get_job_def("harvest_source")

    all_partitions = sources_partitions_def.get_partition_keys(
        dynamic_partitions_store=instance
    )
    assert len(all_partitions) > 0, "Partitions were not generated"

    result = resolved_job.execute_in_process(
        instance=instance, partition_key="ref_mainstems_mainstems__0"
    )

    assert result.success, "Job execution failed for partition 'mainstems__0'"

    query = """
    select * where {
        ?s ?p <https://schema.org/Place> .
    } limit 100
    """
    resultDict = execute_sparql(query)
    assert len(resultDict) > 0
    assert "https://geoconnex.us/ref/mainstems/65655" in resultDict["s"]

    query = """
    select * where {
        <https://geoconnex.us/ref/mainstems/65655> <https://schema.org/name> ?o .
    } limit 100
    """

    resultDict = execute_sparql(query)
    assert len(resultDict) > 0
    assert "Junction Creek" in resultDict["o"]

    result = resolved_job.execute_in_process(
        instance=instance, partition_key="ref_dams_dams__0"
    )
    assert result.success, "Job execution failed for partition 'ref_dams_dams__0'"

    query = """
    select * where {
        <https://geoconnex.us/ref/dams/1045939> <https://schema.org/name> ?o .
    } limit 100
    """

    resultDict = execute_sparql(query)
    assert len(resultDict) > 0
    assert "Upper Railroad" in resultDict["o"]


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

    # Make sure that
    partitionsAfterDelete = instance.get_dynamic_partitions("sources_partitions_def")
    assert "ref_mainstems_mainstems__0" not in partitionsAfterDelete
    assert "ref_dams_dams__0" in partitionsAfterDelete
    assert len(partitionsAfterDelete) == len(newPartitions) - 1
    for key in mocked_partition_keys:
        assert key not in partitionsAfterDelete
