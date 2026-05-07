# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import os
from xml.etree import ElementTree as ET

from dagster import (
    AssetExecutionContext,
    BackfillPolicy,
    asset,
    get_dagster_logger,
)
import docker
import requests

from userCode.lib.dagster import filter_partitions
from userCode.lib.env import (
    MAINSTEM_FILE,
    NABU_IMAGE,
    SITEMAP_INDEX,
)
from userCode.lib.utils import (
    template_rclone,
)

"""
All assets in this asset group set up config needed for crawling, 
generating release graphs, or the dagster instance itself
"""

CONFIG_GROUP = "config"


@asset(group_name=CONFIG_GROUP)
def mainstem_catchment_metadata():
    """
    Download the geoconnex mainstem catchment fgb metadata file locally
    using streaming. This file can be used for adding mainstems to the
    harvested nquads later in the pipeline
    """
    if os.environ.get("GITHUB_ACTIONS") or os.environ.get("PYTEST_CURRENT_TEST"):
        get_dagster_logger().info(
            "Skipping mainstem catchment metadata download in test mode"
        )
        return

    url = (
        "https://storage.googleapis.com/"
        "national-hydrologic-geospatial-fabric-reference-hydrofabric/"
        "reference_catchments_and_flowlines.fgb"
    )
    ONE_MB = 1024 * 1024

    if MAINSTEM_FILE.exists():
        get_dagster_logger().info(
            f"File {MAINSTEM_FILE} already exists; skipping download"
        )
        return

    FIFTEEN_MINUTES = 60 * 15
    get_dagster_logger(f"Downloading {url} to {MAINSTEM_FILE.absolute()} ...")

    LOG_EVERY_BYTES = 250 * ONE_MB
    bytes_downloaded = 0
    next_log_threshold = LOG_EVERY_BYTES

    with requests.get(url, stream=True, timeout=FIFTEEN_MINUTES) as response:
        response.raise_for_status()

        with MAINSTEM_FILE.open("wb") as f:
            for chunk in response.iter_content(chunk_size=ONE_MB):
                if not chunk:  # filter out keep-alive chunks
                    continue

                f.write(chunk)
                bytes_downloaded += len(chunk)

                if bytes_downloaded >= next_log_threshold:
                    get_dagster_logger().info(
                        f"Downloaded {bytes_downloaded / (1024**3):.2f} GB so far..."
                    )
                    next_log_threshold += LOG_EVERY_BYTES


@asset(backfill_policy=BackfillPolicy.single_run(), group_name=CONFIG_GROUP)
def rclone_config() -> str:
    """Create the rclone config by templating the rclone.conf.j2 template"""
    get_dagster_logger().info("Creating rclone config")
    input_file = os.path.join(
        os.path.dirname(__file__), "..", "templates", "rclone.conf.j2"
    )
    templated_conf: str = template_rclone(input_file)
    get_dagster_logger().info(templated_conf)
    return templated_conf


GEOCONNEX_NS = "https://geoconnex.us"
SITEMAP_NS = "http://www.sitemaps.org/schemas/sitemap/0.9"

NS = {
    "sm": SITEMAP_NS,
    "geoconnex": GEOCONNEX_NS,
}


@asset(backfill_policy=BackfillPolicy.single_run(), group_name=CONFIG_GROUP)
def sitemap_partitions(context: AssetExecutionContext):
    """Generate a dynamic partition for each geoconnex:sitemap_id in the sitemap index."""

    r = requests.get(SITEMAP_INDEX, timeout=20)
    r.raise_for_status()

    root = ET.fromstring(r.text)

    names: set[str] = set()

    sitemap_ids = root.findall(
        "sm:sitemap/geoconnex:sitemap_id",
        namespaces=NS,
    )

    assert sitemap_ids, f"No geoconnex:sitemap_id values found in index {SITEMAP_INDEX}"

    for elem in sitemap_ids:
        if elem.text is None:
            raise ValueError(f"Empty geoconnex:sitemap_id found in {SITEMAP_INDEX}")

        name = elem.text.strip()

        if not name:
            raise ValueError(f"Empty geoconnex:sitemap_id found in {SITEMAP_INDEX}")

        if name in names:
            get_dagster_logger().warning(
                f"Found duplicate sitemap_id '{name}' in "
                f"{SITEMAP_INDEX}. Skipping duplicate."
            )
            continue

        get_dagster_logger().info(f"Adding partition {name}")
        names.add(name)

    filter_partitions(context.instance, "sources_partitions_def", names)

    # Each sitemap_id is a partition that can be crawled independently
    context.instance.add_dynamic_partitions(
        partitions_def_name="sources_partitions_def",
        partition_keys=sorted(names),
    )


@asset(backfill_policy=BackfillPolicy.single_run(), group_name=CONFIG_GROUP)
def docker_client_environment():
    """Set up dagster by pulling both the gleaner and nabu images and moving the config files into docker configs"""
    get_dagster_logger().info("Initializing docker client and pulling images: ")
    client = docker.DockerClient()

    get_dagster_logger().info(f"Pulling {NABU_IMAGE}")
    client.images.pull(NABU_IMAGE)
