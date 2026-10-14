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
from minio.error import S3Error
import requests

from userCode.assetGroups.processed import REFERENCE_CATCHMENTS_PATH_IN_S3
from userCode.lib.classes import S3
from userCode.lib.dagster import (
    bulk_sources_partitions_def,
    filter_partitions,
    sources_partitions_def,
)
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
harvesting, or the dagster instance itself
"""

CONFIG_GROUP = "config"


@asset(group_name=CONFIG_GROUP)
def mainstem_catchment_metadata():
    """
    Download the catchment to mainstem flatgeobuf that the
    reference_catchments_and_mainstems asset publishes to the object store.
    This file is used for adding mainstems to the harvested features during
    the harvest
    """
    if os.environ.get("GITHUB_ACTIONS") or os.environ.get("PYTEST_CURRENT_TEST"):
        get_dagster_logger().info(
            "Skipping mainstem catchment metadata download in test mode"
        )
        return

    s3 = S3()
    try:
        remote = s3.client.stat_object(s3.bucket, REFERENCE_CATCHMENTS_PATH_IN_S3)
    except S3Error as e:
        if e.code != "NoSuchKey":
            raise
        if MAINSTEM_FILE.exists():
            get_dagster_logger().warning(
                f"{REFERENCE_CATCHMENTS_PATH_IN_S3} is not in bucket '{s3.bucket}'; "
                f"using the existing local file {MAINSTEM_FILE}"
            )
            return
        raise Exception(
            f"{REFERENCE_CATCHMENTS_PATH_IN_S3} is not in bucket '{s3.bucket}'; "
            "materialize the reference_catchments_and_mainstems asset to generate "
            f"it, or put a copy of it at {MAINSTEM_FILE}"
        ) from e

    # the etag of the object the local file was downloaded from, so the file
    # is downloaded again whenever the processed file is regenerated
    etag_file = MAINSTEM_FILE.with_name(f"{MAINSTEM_FILE.name}.etag")
    if (
        MAINSTEM_FILE.exists()
        and etag_file.exists()
        and etag_file.read_text() == remote.etag
    ):
        get_dagster_logger().info(
            f"File {MAINSTEM_FILE} is already up to date; skipping download"
        )
        return

    get_dagster_logger().info(
        f"Downloading {REFERENCE_CATCHMENTS_PATH_IN_S3} ({remote.size} bytes) "
        f"to {MAINSTEM_FILE.absolute()} ..."
    )
    # fget_object downloads to a temporary file and then renames it, so the
    # asset server never serves a partially downloaded file
    s3.client.fget_object(
        s3.bucket, REFERENCE_CATCHMENTS_PATH_IN_S3, str(MAINSTEM_FILE)
    )
    assert remote.etag
    etag_file.write_text(remote.etag)


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
    """Generate a dynamic partition for each geoconnex:sitemap_id in the sitemap index.
    Sitemaps with a geoconnex:bulk_container_image are put in a separate partition since
    they are harvested by a different asset than sitemaps which crawl features / apis"""

    r = requests.get(SITEMAP_INDEX, timeout=20)
    r.raise_for_status()

    root = ET.fromstring(r.text)

    names: set[str] = set()
    bulk_names: set[str] = set()

    sitemaps = [
        sitemap
        for sitemap in root.findall("sm:sitemap", namespaces=NS)
        if sitemap.find("geoconnex:sitemap_id", namespaces=NS) is not None
    ]

    assert sitemaps, f"No geoconnex:sitemap_id values found in index {SITEMAP_INDEX}"

    for sitemap in sitemaps:
        elem = sitemap.find("geoconnex:sitemap_id", namespaces=NS)
        assert elem is not None
        if elem.text is None:
            raise ValueError(f"Empty geoconnex:sitemap_id found in {SITEMAP_INDEX}")

        name = elem.text.strip()

        if not name:
            raise ValueError(f"Empty geoconnex:sitemap_id found in {SITEMAP_INDEX}")

        if name in names or name in bulk_names:
            get_dagster_logger().warning(
                f"Found duplicate sitemap_id '{name}' in "
                f"{SITEMAP_INDEX}. Skipping duplicate."
            )
            continue

        if sitemap.find("geoconnex:bulk_container_image", namespaces=NS) is not None:
            get_dagster_logger().info(f"Adding bulk partition {name}")
            bulk_names.add(name)
        else:
            get_dagster_logger().info(f"Adding partition {name}")
            names.add(name)

    # Each sitemap_id is a partition that can be crawled independently
    for partitions_def, keys in (
        (sources_partitions_def, names),
        (bulk_sources_partitions_def, bulk_names),
    ):
        assert partitions_def.name
        filter_partitions(context.instance, partitions_def.name, keys)
        context.instance.add_dynamic_partitions(
            partitions_def_name=partitions_def.name,
            partition_keys=sorted(keys),
        )


@asset(backfill_policy=BackfillPolicy.single_run(), group_name=CONFIG_GROUP)
def docker_client_environment():
    """Set up dagster by pulling both the gleaner and nabu images and moving the config files into docker configs"""
    get_dagster_logger().info("Initializing docker client and pulling images: ")
    client = docker.DockerClient()

    get_dagster_logger().info(f"Pulling {NABU_IMAGE}")
    client.images.pull(NABU_IMAGE)
