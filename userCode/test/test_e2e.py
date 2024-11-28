from dagster import load_assets_from_modules, materialize
import userCode.main as main


from dagster import AssetsDefinition, AssetSpec, SourceAsset


def test_materialize_configs():
    assets = load_assets_from_modules([main])
    # It is possible to load certain asset types that cannot be passed into
    # Materialize so we filter them to avoid a pyright type error
    filtered_assets = [
        asset
        for asset in assets
        if isinstance(asset, (AssetsDefinition, AssetSpec, SourceAsset))
    ]
    result = materialize(
        assets=filtered_assets,
        selection=["nabu_config", "gleaner_config", "docker_client_environment"],
    )
    assert result.success
