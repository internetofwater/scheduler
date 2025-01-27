import os
import pytest


@pytest.fixture(scope="session", autouse=True)
def setup_before_tests():
    os.environ["DAGSTER_POSTGRES_HOST"] = "localhost"
