from dagster import load_assets_from_modules, materialize
import userCode.main as main


def test_materialize_configs():
    result = materialize(
        assets=load_assets_from_modules([main]),  # type: ignore
        selection=["nabu_config", "gleaner_config", "docker_client_environment"],
    )
    assert result.success

