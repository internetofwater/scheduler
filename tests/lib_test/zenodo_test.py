# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import pytest
import requests

from userCode.exports import nquads_to_zenodo
from userCode.lib.classes import S3
from userCode.lib.env import ZENODO_ACCESS_TOKEN, ZENODO_SANDBOX_ACCESS_TOKEN


@pytest.mark.skipif(
    ZENODO_ACCESS_TOKEN == "unset", reason="secret access key is not set"
)
def test_zenodo():
    r = requests.get(
        "https://zenodo.org/api/deposit/depositions",
        params={"access_token": ZENODO_ACCESS_TOKEN},
    )
    assert r.ok, r.text


@pytest.mark.skipif(
    ZENODO_SANDBOX_ACCESS_TOKEN == "unset", reason="secret access key is not set"
)
def test_export_zenodo_in_sandbox_environment():
    """Make sure our logic for uploading to zenodo works by uploading a file to s3 and then streaming it to the zenodo sandbox env"""
    objNameInS3 = "fileIdentifier"
    S3().load(b"test", objNameInS3)
    nquads_to_zenodo(
        None,
        export_graph_as_nquads=objNameInS3,
    )
