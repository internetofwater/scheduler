# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

from dagster import get_dagster_logger

"""
Runtime config and env vars for dagster; prioritizes strict env vars
that fail immediately if missing instead of later in the run
"""


def RUNNING_AS_TEST_OR_DEV():
    """Check if we are running outside of the docker container"""
    return "DAGSTER_IS_DEV_CLI" in os.environ or "PYTEST_CURRENT_TEST" in os.environ


def assert_all_vars():
    """assert that all required env vars are set"""
    vars = [
        "GLEANERIO_MINIO_ADDRESS",
        "GLEANERIO_MINIO_PORT",
        "GLEANERIO_MINIO_USE_SSL",
        # these are named differently since they are shared between user code and the minio container
        "MINIO_SECRET_KEY",
        "MINIO_ACCESS_KEY",
        "GLEANERIO_MINIO_BUCKET",
        "GLEANER_HEADLESS_ENDPOINT",  # note this is named differently, confusingly so does not have the IO
        "GLEANERIO_GRAPH_URL",
        "LAKEFS_ENDPOINT_URL",
        "LAKEFS_ACCESS_KEY_ID",
        "LAKEFS_SECRET_ACCESS_KEY",
        "GLEANER_THREADS",
    ]
    errors = ""
    for var in vars:
        if os.environ.get(var) is None:
            errors += f"Missing {var}, "
        elif os.environ.get(var) == "unset":
            get_dagster_logger().warning(
                f"Unset env var: {var}. This is likely a secret key. It is ok to ignore this warning if you do not intend on using private API endpoints, i.e. in CI/CD"
            )
    if errors:
        raise Exception(errors)


assert_all_vars()


def strict_env_int(key: str):
    """Get an env var and ensure it is an int"""
    val = os.environ.get(key)
    if val is None:
        raise Exception(f"Missing {key}")

    return int(val)


def strict_env(key: str):
    """Get an env var and ensure it is a string"""
    val = os.environ.get(key)
    if val is None:
        raise Exception(f"Missing {key}")

    return val


RELEASE_PATH = "graphs/latest"
GLEANER_HEADLESS_NETWORK = "headless_gleanerio"
GLEANER_MINIO_ADDRESS = strict_env("GLEANERIO_MINIO_ADDRESS")
GLEANER_MINIO_PORT = strict_env("GLEANERIO_MINIO_PORT")
GLEANER_MINIO_USE_SSL = strict_env("GLEANERIO_MINIO_USE_SSL") in [
    True,
    "true",
    "True",
]
GLEANER_MINIO_SECRET_KEY = strict_env("MINIO_SECRET_KEY")
GLEANER_MINIO_ACCESS_KEY = strict_env("MINIO_ACCESS_KEY")
GLEANER_MINIO_BUCKET = strict_env("GLEANERIO_MINIO_BUCKET")
# set for the earhtcube utiltiies
MINIO_OPTIONS = {
    "secure": GLEANER_MINIO_USE_SSL,
    "access_key": GLEANER_MINIO_ACCESS_KEY,
    "secret_key": GLEANER_MINIO_SECRET_KEY,
}

GLEANER_HEADLESS_ENDPOINT = strict_env("GLEANER_HEADLESS_ENDPOINT")
# using GLEANER, even though this is a nabu property... same prefix seems easier
GLEANER_GRAPH_URL = strict_env("GLEANERIO_GRAPH_URL")
GLEANER_IMAGE = strict_env("GLEANER_IMAGE")
NABU_IMAGE = strict_env("NABU_IMAGE")
GLEANERIO_DATAGRAPH_ENDPOINT = strict_env("GLEANERIO_DATAGRAPH_ENDPOINT")
GLEANERIO_PROVGRAPH_ENDPOINT = strict_env("GLEANERIO_PROVGRAPH_ENDPOINT")
REMOTE_GLEANER_SITEMAP = strict_env("REMOTE_GLEANER_SITEMAP")
LAKEFS_ENDPOINT_URL = strict_env("LAKEFS_ENDPOINT_URL")
LAKEFS_ACCESS_KEY_ID = strict_env("LAKEFS_ACCESS_KEY_ID")
LAKEFS_SECRET_ACCESS_KEY = strict_env("LAKEFS_SECRET_ACCESS_KEY")

userCodeRoot = Path(__file__).parent.parent.parent.absolute()

DAGSTER_YAML_CONFIG: str = os.path.join(userCodeRoot, "dagster.yaml")

assert Path(DAGSTER_YAML_CONFIG).exists(), (
    f"the dagster.yaml file does not exist at {DAGSTER_YAML_CONFIG}"
)
assert Path("/tmp/geoconnex/").exists(), (
    "the /tmp/geoconnex directory does not exist. This must exist for us to share configs with the docker socket on the host"
)

NABU_PROFILING = strict_env("NABU_PROFILING")
