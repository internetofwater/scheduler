import os
import pytest


@pytest.fixture(scope="session", autouse=True)
def setup_before_tests():
    # assume we are not inside the compose project if we are testing and thus
    # we want to use localhost to connect
    os.environ["DAGSTER_POSTGRES_HOST"] = "localhost"
