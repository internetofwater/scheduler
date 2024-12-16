import os
from dagster import materialize_to_memory
import requests

from userCode.lib.classes import S3, RClone
from userCode.lib.env import (
    RCLONE_ACCESS_KEY_ID,
    RCLONE_ENDPOINT_URL,
    RCLONE_SECRET_ACCESS_KEY,
    strict_env,
)
from userCode.main import rclone_config


def test_rclone_installed():
    # make sure you can run rclone version
    assert os.system("rclone version") == 0


def test_env_vars():
    """for every env var, make sure that there are no "" values which signify
    env vars that were incorrectly applied or missing"""
    env = os.environ
    for key in env.keys():
        assert (
            env[key] != ""
        ), "{} is empty, but scheduler should only be using env vars that are defined".format(
            key
        )


def test_lakefs_health():
    """Ensure we can connect to the remote lakefs cluster"""
    RCLONE_ENDPOINT_URL = strict_env("RCLONE_ENDPOINT_URL")

    response = requests.get(
        f"{RCLONE_ENDPOINT_URL}/healthcheck",
    )

    assert (
        response.status_code == 204
    ), f"{RCLONE_ENDPOINT_URL} is not healthy: {response.text}"


def test_rclone_config_location():
    location = RClone.get_config_path()
    assert location.parent.exists()


def test_access_lakefs_repo():
    response = requests.get(
        f"{RCLONE_ENDPOINT_URL}/repositories/geoconnex",
        auth=(RCLONE_ACCESS_KEY_ID, RCLONE_SECRET_ACCESS_KEY),
    )
    json = response.json()
    assert response.status_code == 200, f"{RCLONE_ENDPOINT_URL} is not healthy: {json}"

    assert json["id"] == "geoconnex"
    assert json["storage_namespace"] == "local://geoconnex"
    assert json["default_branch"] == "main"
    assert json["read_only"] is False


def test_rclone_s3_to_lakefs():
    client = S3()
    arbitary_dummy_data = b"TEST_S3_DATA_THAT_SHOULD_GET_UPLOADED"
    client.load(arbitary_dummy_data, "test_file.json")

    assert client.read("test_file.json") == arbitary_dummy_data

    result = materialize_to_memory(assets=[rclone_config])
    assert result.success
    rclone_client = RClone(config_data=result.output_for_node("rclone_config"))
    rclone_client.copy("test_file.json")
