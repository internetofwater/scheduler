# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

"""
Runtime config and env vars for dagster; prioritizes strict env vars
that fail immediately if missing instead of later in the run
"""


def RUNNING_AS_TEST_OR_DEV():
    """Check if we are running outside of the docker container"""
    return "DAGSTER_IS_DEV_CLI" in os.environ or "PYTEST_CURRENT_TEST" in os.environ


def strict_env_int(key: str) -> int:
    """Get an env var and ensure it is an int"""
    return int(strict_env(key))


def strict_env_bool(key: str) -> bool:
    """Get an env var and ensure it is a bool"""
    return strict_env(key) in ["True", "true", True]


def strict_env(key: str) -> str:
    """Get an env var and ensure it is a string"""
    val = os.environ.get(key)
    if val is None:
        raise Exception(f"Env var '{key}' was not set")

    return val


### Infrastructure and Storage Options
S3_ADDRESS = strict_env("S3_ADDRESS")
S3_PORT = strict_env("S3_PORT")
S3_SECRET_KEY = strict_env("S3_SECRET_KEY")
S3_ACCESS_KEY = strict_env("S3_ACCESS_KEY")
S3_DEFAULT_BUCKET = strict_env("S3_DEFAULT_BUCKET")
S3_USE_SSL = strict_env_bool("S3_USE_SSL")
TRIPLESTORE_URL = strict_env("TRIPLESTORE_URL")
DATAGRAPH_REPOSITORY = strict_env("DATAGRAPH_REPOSITORY")
PROVGRAPH_REPOSITORY = strict_env("PROVGRAPH_REPOSITORY")

### Gleaner Options
HEADLESS_ENDPOINT = strict_env("HEADLESS_ENDPOINT")
GLEANER_LOG_LEVEL = strict_env("GLEANER_LOG_LEVEL")
GLEANER_CONCURRENT_SITEMAPS = strict_env_int("GLEANER_CONCURRENT_SITEMAPS")
GLEANER_SITEMAP_WORKERS = strict_env_int("GLEANER_SITEMAP_WORKERS")
GLEANER_SITEMAP_INDEX = strict_env("GLEANER_SITEMAP_INDEX")

### Nabu Options
NABU_IMAGE = strict_env("NABU_IMAGE")
NABU_PROFILING = strict_env_bool("NABU_PROFILING")
NABU_BATCH_SIZE = strict_env_int("NABU_BATCH_SIZE")
NABU_LOG_LEVEL = strict_env("NABU_LOG_LEVEL")

### Export Options
LAKEFS_ENDPOINT_URL = strict_env("LAKEFS_ENDPOINT_URL")
LAKEFS_ACCESS_KEY_ID = strict_env("LAKEFS_ACCESS_KEY_ID")
LAKEFS_SECRET_ACCESS_KEY = strict_env("LAKEFS_SECRET_ACCESS_KEY")
ZENODO_ACCESS_TOKEN = strict_env("ZENODO_ACCESS_TOKEN")
ZENODO_SANDBOX_ACCESS_TOKEN = strict_env("ZENODO_SANDBOX_ACCESS_TOKEN")

### Dagster Options
repositoryRoot = Path(__file__).parent.parent.parent.absolute()
DAGSTER_YAML_CONFIG: str = os.path.join(repositoryRoot, "dagster.yaml")

assert Path(DAGSTER_YAML_CONFIG).exists(), (
    f"the dagster.yaml file does not exist at {DAGSTER_YAML_CONFIG}"
)
