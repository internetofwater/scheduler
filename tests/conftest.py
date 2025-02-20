# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import os
import pytest


# These are needed given the fact that the tests are always best ran outside the docker
# container. We set this here since we don't want to put it in the .env necessarily
@pytest.fixture(scope="session", autouse=True)
def setup_before_tests():
    # assume we are not inside the compose project if we are testing and thus
    # we want to use localhost to connect. We have to set this here since
    # we don't want to put it in the .env necessarily
    os.environ["DAGSTER_POSTGRES_HOST"] = "localhost"
    os.environ["DAGSTER_HOME"] = os.path.join(os.path.dirname(__file__), "..", "..")
