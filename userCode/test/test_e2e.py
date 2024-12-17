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
