import os
from dagster import materialize_to_memory
import lakefs
import requests

from userCode.lib.classes import S3, FileTransferer
from userCode.lib.env import (
    LAKEFS_ACCESS_KEY_ID,
    LAKEFS_ENDPOINT_URL,
    LAKEFS_SECRET_ACCESS_KEY,
    strict_env,
)
from userCode.main import rclone_config


def test_rclone_installed():
    """make sure you can run rclone version"""
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
    LAKEFS_ENDPOINT_URL = strict_env("LAKEFS_ENDPOINT_URL")

    response = requests.get(
        f"{LAKEFS_ENDPOINT_URL}/api/v1/healthcheck",
    )

    assert (
        response.status_code == 204
    ), f"{LAKEFS_ENDPOINT_URL} is not healthy: {response.text}"


def test_rclone_config_location():
    """Make sure we can find the rclone config file" locally"""
    location = FileTransferer.get_rclone_config_path()
    assert location.parent.exists()


def test_access_lakefs_repo():
    """Make sure that the geoconnex repo exists on the remote"""
    response = requests.get(
        f"{LAKEFS_ENDPOINT_URL}/api/v1/repositories/geoconnex",
        auth=(LAKEFS_ACCESS_KEY_ID, LAKEFS_SECRET_ACCESS_KEY),
    )
    json = response.json()
    assert response.status_code == 200, f"{LAKEFS_ENDPOINT_URL} is not healthy: {json}"

    assert json["id"] == "geoconnex"
    assert json["storage_namespace"] == "local://geoconnex"
    assert json["default_branch"] == "main"
    assert json["read_only"] is False


def test_rclone_s3_to_lakefs():
    """Make sure you can transfer a json file from s3 to lakefs"""
    client = S3()
    arbitary_dummy_data = b"TEST_S3_DATA_THAT_SHOULD_GET_UPLOADED"
    client.load(arbitary_dummy_data, "test_file.json")

    assert client.read("test_file.json") == arbitary_dummy_data

    result = materialize_to_memory(assets=[rclone_config])
    assert result.success
    rclone_client = FileTransferer(config_data=result.output_for_node("rclone_config"))
    rclone_client.copy("test_file.json")

    test_file = (
        lakefs.repository("geoconnex", client=rclone_client.lakefs_client)
        .branch("main")
        .object("test_file.json")
    )
    assert test_file.exists()
    test_file.delete()
    assert test_file.exists() is False
