# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from dagster import materialize_to_memory
import lakefs
import requests
import pytest
from userCode.lib.classes import (
    S3,
    FileTransferer,
)
from userCode.lib.env import (
    LAKEFS_ACCESS_KEY_ID,
    LAKEFS_ENDPOINT_URL,
    LAKEFS_SECRET_ACCESS_KEY,
)
from userCode.lib.lakefs import LakeFSClient
from userCode.pipeline import rclone_config


@pytest.fixture
def lakefs_client() -> LakeFSClient:
    return LakeFSClient("geoconnex")


@pytest.mark.skipif(
    LAKEFS_SECRET_ACCESS_KEY == "unset", reason="secret access key is not set"
)
def test_upstream_lakefs_health():
    """Ensure we can connect to the remote lakefs cluster"""

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


@pytest.mark.skipif(
    LAKEFS_SECRET_ACCESS_KEY == "unset", reason="secret access key is not set"
)
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


@pytest.mark.skipif(
    LAKEFS_SECRET_ACCESS_KEY == "unset", reason="secret access key is not set"
)
def test_rclone_s3_to_lakefs(lakefs_client: LakeFSClient):
    """Make sure you can transfer a json file from s3 to lakefs"""
    s3_client = S3()
    arbitary_dummy_data = b"TEST_S3_DATA_THAT_SHOULD_GET_UPLOADED"
    filename = "test.txt"
    s3_client.load(arbitary_dummy_data, filename)

    assert s3_client.read(filename) == arbitary_dummy_data

    result = materialize_to_memory(assets=[rclone_config])
    assert result.success
    rclone_client = FileTransferer(config_data=result.output_for_node("rclone_config"))
    rclone_client.copy_to_lakefs(
        filename,
        destination_branch="test_branch_for_CI",
        destination_filename=filename,
        lakefs_client=lakefs_client,
    )

    stagingBranch = lakefs.repository(
        "geoconnex", client=rclone_client.lakefs_client
    ).branch("test_branch_for_CI")

    lakefs_client.assert_file_exists(filename, "test_branch_for_CI")
    stagingBranch.object(filename).delete()

    stagingBranch.commit(
        message=f"Cleaning up after CI/CD tests and deleting {filename}"
    )

    stagingBranch.delete()


@pytest.mark.skipif(
    LAKEFS_SECRET_ACCESS_KEY == "unset", reason="secret access key is not set"
)
def test_branch_ops(lakefs_client: LakeFSClient):
    lakefs_client.create_branch_if_not_exists("dummy_empty_test_branch")
    branch = lakefs_client.get_branch("dummy_empty_test_branch")
    assert branch
    branch.delete()
    assert not lakefs_client.get_branch("dummy_empty_test_branch")
