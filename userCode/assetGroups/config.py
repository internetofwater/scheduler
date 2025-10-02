# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import os

from bs4 import BeautifulSoup, ResultSet
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    BackfillPolicy,
    asset,
    asset_check,
    get_dagster_logger,
)
import docker
import requests

from userCode.lib.dagster import filter_partitions
from userCode.lib.env import (
    GLEANER_SITEMAP_INDEX,
    HEADLESS_ENDPOINT,
    NABU_IMAGE,
    RUNNING_AS_TEST_OR_DEV,
)
from userCode.lib.utils import (
    template_rclone,
)

"""
All assets in this asset group set up config needed for crawling, 
syncing, or the dagster instance itself
"""

CONFIG_GROUP = "config"


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
    get_dagster_logger().info("Creating gleaner config")

    r = requests.get(GLEANER_SITEMAP_INDEX)
    r.raise_for_status()
    xml = r.text

    if not os.path.exists("/tmp/geoconnex"):
        os.mkdir("/tmp/geoconnex")
    # write the sitemap to disk as a cache, that
    # way if the sitemap index is huge, we don't have to download it every time
    with open("/tmp/geoconnex/sitemap.xml", "w") as f:
        f.write(xml)

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


@asset_check(asset=docker_client_environment)
def can_contact_headless():
    """Check that we can contact the headless server"""
    TWO_SECONDS = 2

    url = HEADLESS_ENDPOINT
    if RUNNING_AS_TEST_OR_DEV():
        portNumber = HEADLESS_ENDPOINT.removeprefix("http://").split(":")[1]
        url = f"http://localhost:{portNumber}"
        get_dagster_logger().warning(
            f"Skipping headless check in test mode. Check would have pinged {url}"
        )
        # Dagster does not support skipping asset checks so must return a valid result
        return AssetCheckResult(passed=True)

    # the Host header needs to be set for Chromium due to an upstream security requirement
    result = requests.get(url, headers={"Host": "localhost"}, timeout=TWO_SECONDS)
    return AssetCheckResult(
        passed=True,
        metadata={
            "status_code": result.status_code,
            "text": result.text,
            "endpoint": HEADLESS_ENDPOINT,
        },
    )
