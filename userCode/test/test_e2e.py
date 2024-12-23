from dagster import (
    DagsterInstance,
    load_assets_from_modules,
    materialize,
)
import userCode.main as main
from userCode.main import definitions, sources_partitions_def


from dagster import AssetsDefinition, AssetSpec, SourceAsset


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
        instance=instance, partition_key="ref_hu02_hu02__0"
    )

    assert result.success, "Job execution failed for partition 'ref_hu02_hu02__0'"


def test_dynamic_partitions():
    """Make sure that a new materialization of the gleaner config will create new partitions"""
    instance = DagsterInstance.ephemeral()
    dummy_names = ["test_partition1", "test_partition2", "test_partition3"]
    instance.add_dynamic_partitions(
        partitions_def_name="sources_partitions_def", partition_keys=list(dummy_names)
    )

    assert instance.get_dynamic_partitions("sources_partitions_def") == dummy_names

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
    assert result.success, "Expected gleane config to materialize"

    assert instance.get_dynamic_partitions("sources_partitions_def") != dummy_names
    assert "ref_hu02_hu02__0" in instance.get_dynamic_partitions(
        "sources_partitions_def"
    ), "Reference feature hu02 was not found in partitions"
