import os

DEBUG = os.getenv("DEBUG", "False").lower() == "true"
SUMMARY_PATH = "graphs/summary"
RELEASE_PATH = "graphs/latest"
GLEANER_HEADLESS_NETWORK = "headless_gleanerio"
GLEANERIO_LOG_PREFIX = "scheduler/logs/"
GLEANER_CONFIG_PATH = "/opt/dagster/app/build/gleanerconfig.yaml"
if not os.path.exists(GLEANER_CONFIG_PATH):
    raise Exception(
        f"Missing gleaner config file: Not located at {GLEANER_CONFIG_PATH}"
    )
GLEANERIO_GLEANER_CONFIG_PATH = GLEANER_CONFIG_PATH
if not os.path.exists(GLEANERIO_GLEANER_CONFIG_PATH):
    raise Exception(
        f"Missing gleaner config file: Not located at {GLEANERIO_GLEANER_CONFIG_PATH}"
    )
GLEANERIO_NABU_CONFIG_PATH = "/opt/dagster/app/build/nabuconfig.yaml"
if not os.path.exists(GLEANERIO_NABU_CONFIG_PATH):
    raise Exception(
        f"Missing nabu config file: Not located at {GLEANERIO_NABU_CONFIG_PATH}"
    )


## Env vars ##
def assert_all_vars():
    """If env vars aren't defined properly, we want to immediately catch
    this and fail early instead of spawning containers with empty env vars"""
    vars = [
        "GLEANERIO_MINIO_ADDRESS",
        "GLEANERIO_MINIO_PORT",
        "GLEANERIO_MINIO_USE_SSL",
        "GLEANERIO_MINIO_SECRET_KEY",
        "GLEANERIO_MINIO_ACCESS_KEY",
        "GLEANERIO_MINIO_BUCKET",
        "GLEANERIO_HEADLESS_ENDPOINT",
        "GLEANERIO_GRAPH_URL",
        "GLEANERIO_GRAPH_NAMESPACE",
    ]
    errors = ""
    for var in vars:
        if os.environ.get(var) is None:
            errors += f"Missing {var}, "
    if errors:
        raise Exception(errors)


assert_all_vars()


def strict_env(key: str):
    val = os.environ.get(key)
    if val is None:
        raise Exception(f"Missing {key}")

    return val


GLEANER_MINIO_ADDRESS = strict_env("GLEANERIO_MINIO_ADDRESS")
GLEANER_MINIO_PORT = strict_env("GLEANERIO_MINIO_PORT")
GLEANER_MINIO_USE_SSL = strict_env("GLEANERIO_MINIO_USE_SSL") in [
    True,
    "true",
    "True",
]
GLEANER_MINIO_SECRET_KEY = strict_env("GLEANERIO_MINIO_SECRET_KEY")
GLEANER_MINIO_ACCESS_KEY = strict_env("GLEANERIO_MINIO_ACCESS_KEY")
GLEANER_MINIO_BUCKET = strict_env("GLEANERIO_MINIO_BUCKET")
# set for the earhtcube utiltiies
MINIO_OPTIONS = {
    "secure": GLEANER_MINIO_USE_SSL,
    "access_key": GLEANER_MINIO_ACCESS_KEY,
    "secret_key": GLEANER_MINIO_SECRET_KEY,
}

GLEANER_HEADLESS_ENDPOINT = strict_env("GLEANERIO_HEADLESS_ENDPOINT")
# using GLEANER, even though this is a nabu property... same prefix seems easier
GLEANER_GRAPH_URL = strict_env("GLEANERIO_GRAPH_URL")
GLEANER_GRAPH_NAMESPACE = strict_env("GLEANERIO_GRAPH_NAMESPACE")
GLEANERIO_GLEANER_IMAGE = strict_env("GLEANERIO_GLEANER_IMAGE")
GLEANERIO_NABU_IMAGE = strict_env("GLEANERIO_NABU_IMAGE")
GLEANERIO_DATAGRAPH_ENDPOINT = strict_env("GLEANERIO_DATAGRAPH_ENDPOINT")
GLEANERIO_PROVGRAPH_ENDPOINT = strict_env("GLEANERIO_PROVGRAPH_ENDPOINT")
