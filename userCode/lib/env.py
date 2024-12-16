import os

from dagster import OpExecutionContext

"""
Runtime config and env vars for dagster; prioritizes strict env vars
that fail immediately if missing instead of later in the run
"""


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
        "GLEANERIO_GRAPH_NAMESPACE",
        "RCLONE_ENDPOINT_URL",
        "RCLONE_ACCESS_KEY_ID",
        "RCLONE_SECRET_ACCESS_KEY",
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


def strict_get_tag(context: OpExecutionContext, key: str) -> str:
    """Gets a tag from a dagster runand ensures it exists before running further jobs"""
    src = context.run_tags[key]
    if src is None:
        raise Exception(f"Missing run tag {key}")
    return src


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
GLEANER_GRAPH_NAMESPACE = strict_env("GLEANERIO_GRAPH_NAMESPACE")
GLEANERIO_GLEANER_IMAGE = strict_env("GLEANERIO_GLEANER_IMAGE")
GLEANERIO_NABU_IMAGE = strict_env("GLEANERIO_NABU_IMAGE")
GLEANERIO_DATAGRAPH_ENDPOINT = strict_env("GLEANERIO_DATAGRAPH_ENDPOINT")
GLEANERIO_PROVGRAPH_ENDPOINT = strict_env("GLEANERIO_PROVGRAPH_ENDPOINT")
REMOTE_GLEANER_SITEMAP = strict_env("REMOTE_GLEANER_SITEMAP")
RCLONE_ENDPOINT_URL = strict_env("RCLONE_ENDPOINT_URL")
RCLONE_ACCESS_KEY_ID = strict_env("RCLONE_ACCESS_KEY_ID")
RCLONE_SECRET_ACCESS_KEY = strict_env("RCLONE_SECRET_ACCESS_KEY")
