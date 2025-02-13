# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import pytest
import requests

from userCode.lib.env import ZENODO_ACCESS_TOKEN


@pytest.mark.skipif(
    ZENODO_ACCESS_TOKEN == "unset", reason="secret access key is not set"
)
def test_zenodo():
    r = requests.get(
        "https://zenodo.org/api/deposit/depositions",
        params={"access_token": ZENODO_ACCESS_TOKEN},
    )
    assert r.ok, r.text
