# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

from bs4 import BeautifulSoup, ResultSet
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
    GLEANER_SITEMAP_INDEX,
    NABU_IMAGE,
)
from userCode.lib.utils import (
    template_rclone,
)

"""
All assets in this asset group set up config needed for crawling, 
syncing, or the dagster instance itself
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
    output_path = "reference_catchments_and_flowlines.fgb"
    ONE_MB = 1024 * 1024
    output_path = Path(output_path)

    if output_path.exists():
        get_dagster_logger().info(
            f"File {output_path} already exists; skipping download"
        )
        return

    FIFTEEN_MINUTES = 60 * 15
    get_dagster_logger(f"Downloading {url} to {output_path.absolute()} ...")

    LOG_EVERY_BYTES = 250 * ONE_MB
    bytes_downloaded = 0
    next_log_threshold = LOG_EVERY_BYTES

    with requests.get(url, stream=True, timeout=FIFTEEN_MINUTES) as response:
        response.raise_for_status()

        with output_path.open("wb") as f:
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


@asset(backfill_policy=BackfillPolicy.single_run(), group_name=CONFIG_GROUP)
def sitemap_partitions(context: AssetExecutionContext):
    """Generate a dynamic partition for each sitemap in the sitemap index"""

    r = requests.get(GLEANER_SITEMAP_INDEX)
    r.raise_for_status()
    xml = r.text

    sitemapTags: ResultSet = BeautifulSoup(xml, features="xml").find_all("sitemap")
    Lines: list[str] = [
        tag.text for tag in (sitemap.find_next("loc") for sitemap in sitemapTags) if tag
    ]

    names: set[str] = set()

    assert len(Lines) > 0, f"No sitemaps found in index {GLEANER_SITEMAP_INDEX}"

    for line in Lines:
        basename = GLEANER_SITEMAP_INDEX.removesuffix(".xml")
        name = (
            line.removeprefix(basename)
            .removesuffix(".xml")
            .removeprefix("/")
            .removesuffix("/")
            .replace("/", "_")
        )
        if name in names:
            get_dagster_logger().warning(
                f"Found duplicate name '{name}' in line '{line}' in sitemap {GLEANER_SITEMAP_INDEX}. Skipping adding it again"
            )
            continue

        get_dagster_logger().info(f"Adding partition {name}")
        names.add(name)

    filter_partitions(context.instance, "sources_partitions_def", names)

    # Each source is a partition that can be crawled independently
    context.instance.add_dynamic_partitions(
        partitions_def_name="sources_partitions_def", partition_keys=list(names)
    )


@asset(backfill_policy=BackfillPolicy.single_run(), group_name=CONFIG_GROUP)
def docker_client_environment():
    """Set up dagster by pulling both the gleaner and nabu images and moving the config files into docker configs"""
    get_dagster_logger().info("Initializing docker client and pulling images: ")
    client = docker.DockerClient()

    get_dagster_logger().info(f"Pulling {NABU_IMAGE}")
    client.images.pull(NABU_IMAGE)
