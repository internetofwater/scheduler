# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import asyncio
import os
import platform
import shutil
import subprocess
from threading import Thread
import zipfile
from aiohttp import ClientSession
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
import yaml
from userCode.lib.classes import S3
from userCode.lib.containers import GleanerContainer, NabuContainer
from userCode.lib.dagster import filter_partitions
from userCode.lib.env import (
    DATAGRAPH_REPOSITORY,
    GLEANER_IMAGE,
    GLEANER_SITEMAP_INDEX,
    HEADLESS_ENDPOINT,
    NABU_IMAGE,
    PROVGRAPH_REPOSITORY,
    RUNNING_AS_TEST_OR_DEV,
)
from userCode.lib.utils import (
    remove_non_alphanumeric,
    template_rclone,
)
from userCode.lib.dagster import sources_partitions_def

"""
This file defines all of the core assets that make up the
Geoconnex pipeline. It does not deal with external exports
or dagster sensors that trigger it, just the core pipeline
"""


def ensure_local_bin_in_path():
    """Ensure ~/.local/bin is in the PATH."""
    local_bin = os.path.expanduser("~/.local/bin")
    if local_bin not in os.environ["PATH"].split(os.pathsep):
        os.environ["PATH"] += os.pathsep + local_bin
    return local_bin


@asset(backfill_policy=BackfillPolicy.single_run())
def rclone_binary():
    """Download the rclone binary to a user-writable location in the PATH."""
    local_bin = ensure_local_bin_in_path()
    os.makedirs(local_bin, exist_ok=True)

    # Check if rclone is already installed in ~/.local/bin
    rclone_path = os.path.join(local_bin, "rclone")
    if os.path.isfile(rclone_path):
        get_dagster_logger().info(f"Rclone is already installed at {rclone_path}")
        return
    elif os.path.isfile("/usr/bin/rclone"):
        get_dagster_logger().info("Rclone is already installed at /usr/bin/rclone")
        return

    # Determine the platform
    system = platform.system().lower()
    arch = platform.machine().lower()

    # Map system and architecture to the appropriate Rclone download URL
    if system == "linux" and arch in ("x86_64", "amd64"):
        download_url = "https://downloads.rclone.org/rclone-current-linux-amd64.zip"
    elif system == "linux" and arch in ("arm64", "aarch64"):
        download_url = "https://downloads.rclone.org/rclone-current-linux-arm64.zip"
    elif system == "darwin" and arch in ("arm64", "aarch64"):
        download_url = "https://downloads.rclone.org/rclone-current-osx-arm64.zip"
    else:
        raise SystemError(
            "Unsupported system or architecture: {} on {}".format(arch, system)
        )

    # Download the file
    def download_file(url, dest):
        print(f"Downloading Rclone from {url}...")
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(dest, "wb") as f:
                shutil.copyfileobj(response.raw, f)
            print("Download complete.")
        else:
            raise RuntimeError(
                f"Failed to download file. HTTP Status Code: {response.status_code}"
            )

    zip_file = "rclone.zip"
    download_file(download_url, zip_file)

    # Extract the downloaded zip file
    with zipfile.ZipFile(zip_file, "r") as zip_ref:
        print("Extracting Rclone...")
        zip_ref.extractall("rclone_extracted")

    # Change to the extracted directory
    extracted_dir = next(
        (
            d
            for d in os.listdir("rclone_extracted")
            if os.path.isdir(os.path.join("rclone_extracted", d))
        ),
        None,
    )
    if not extracted_dir:
        raise FileNotFoundError("Extracted Rclone directory not found.")

    extracted_path = os.path.join("rclone_extracted", extracted_dir)

    # Copy the Rclone binary to ~/.local/bin
    rclone_binary = os.path.join(extracted_path, "rclone")
    if not os.path.isfile(rclone_binary):
        raise FileNotFoundError("Rclone binary not found in extracted directory.")

    print(f"Installing Rclone to {local_bin}...")
    shutil.copy(rclone_binary, rclone_path)
    os.chmod(rclone_path, 0o755)  # Set executable permissions

    print("Verifying Rclone installation...")
    subprocess.run(["rclone", "version"], check=True)

    os.remove(zip_file)
    shutil.rmtree("rclone_extracted")
    print("Installation complete.")


@asset(deps=[rclone_binary], backfill_policy=BackfillPolicy.single_run())
def rclone_config() -> str:
    """Create the rclone config by templating the rclone.conf.j2 template"""
    get_dagster_logger().info("Creating rclone config")
    input_file = os.path.join(os.path.dirname(__file__), "templates", "rclone.conf.j2")
    templated_conf: str = template_rclone(input_file)
    get_dagster_logger().info(templated_conf)
    return templated_conf


@asset(backfill_policy=BackfillPolicy.single_run())
def gleaner_partitions(context: AssetExecutionContext):
    """The gleanerconfig.yaml used for gleaner"""
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

    sources = []
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
        name = remove_non_alphanumeric(name)
        if name in names:
            get_dagster_logger().warning(
                f"Found duplicate name '{name}' in line '{line}' in sitemap {GLEANER_SITEMAP_INDEX}. Skipping adding it again"
            )
            continue

        names.add(name)

    get_dagster_logger().info(f"Found {len(sources)} sources in the sitemap")
    filter_partitions(context.instance, "sources_partitions_def", names)

    # Each source is a partition that can be crawled independently
    context.instance.add_dynamic_partitions(
        partitions_def_name="sources_partitions_def", partition_keys=list(names)
    )


@asset_check(asset=gleaner_partitions)
def gleaner_links_are_valid():
    """Check if all the links in the gleaner config are valid and validate all 'loc' tags in the XML at each UR"""
    s3_client = S3()
    config = s3_client.read("configs/gleanerconfig.yaml")
    yaml_config = yaml.safe_load(config)

    dead_links: list[dict[str, int]] = []

    async def validate_url(url: str):
        async with ClientSession() as session:
            # only request the headers of each geoconnex sitemap
            # no reason to download all the content
            async with session.head(url) as response:
                if response.status == 200:
                    get_dagster_logger().debug(f"URL {url} exists.")
                else:
                    get_dagster_logger().debug(
                        f"URL {url} returned status code {response.status}."
                    )
                    dead_links.append({url: response.status})

    async def main(urls):
        tasks = [validate_url(url) for url in urls]
        results = await asyncio.gather(*tasks)
        return results

    urls = [source["url"] for source in yaml_config["sources"]]
    asyncio.run(main(urls))

    return AssetCheckResult(
        passed=len(dead_links) == 0,
        metadata={
            "failed_urls": list(dead_links),
        },
    )


@asset(backfill_policy=BackfillPolicy.single_run())
def docker_client_environment():
    """Set up dagster by pulling both the gleaner and nabu images and moving the config files into docker configs"""
    get_dagster_logger().info("Initializing docker client and pulling images: ")
    client = docker.DockerClient()

    get_dagster_logger().info(f"Pulling {GLEANER_IMAGE} and {NABU_IMAGE}")

    # Use threading since pull is not async and we want to do both at the same time
    gleaner_thread = Thread(target=client.images.pull, args=(GLEANER_IMAGE,))
    nabu_thread = Thread(target=client.images.pull, args=(NABU_IMAGE,))
    gleaner_thread.start()
    nabu_thread.start()
    gleaner_thread.join()
    nabu_thread.join()


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
        passed=result.status_code == 200,
        metadata={
            "status_code": result.status_code,
            "text": result.text,
            "endpoint": HEADLESS_ENDPOINT,
        },
    )


@asset(
    partitions_def=sources_partitions_def,
    deps=[docker_client_environment, gleaner_partitions],
)
def gleaner(context: AssetExecutionContext):
    """Get the jsonld for each site in the gleaner config"""
    GleanerContainer(context.partition_key).run()


@asset(partitions_def=sources_partitions_def, deps=[gleaner])
def nabu_sync(context: AssetExecutionContext):
    """Synchronize the graph with s3 by adding/removing from the graph"""
    NabuContainer("sync", context.partition_key).run(
        f"sync --prefix summoned/{context.partition_key} --repository {DATAGRAPH_REPOSITORY}"
    )


@asset(partitions_def=sources_partitions_def, deps=[gleaner])
def nabu_prov_release(context: AssetExecutionContext):
    """Construct an nq file from all of the jsonld prov produced by gleaner.
    Used for tracing data lineage"""
    NabuContainer("prov-release", context.partition_key).run(
        f"release --prefix prov/{context.partition_key}"
    )


@asset(partitions_def=sources_partitions_def, deps=[nabu_prov_release])
def nabu_prov_object(context: AssetExecutionContext):
    """Take the nq file from s3 and use the sparql API to upload it into the prov graph repository"""
    NabuContainer("prov-object", context.partition_key).run(
        f"object graphs/latest/{context.partition_key}_prov.nq --repository {PROVGRAPH_REPOSITORY}"
    )


@asset(partitions_def=sources_partitions_def, deps=[gleaner])
def nabu_orgs_release(context: AssetExecutionContext):
    """Construct an nq file for the metadata of an organization. The metadata about their water data is not included in this step.
    This is just flat metadata"""
    NabuContainer("orgs-release", context.partition_key).run(
        f"release --prefix orgs/{context.partition_key} --repository {DATAGRAPH_REPOSITORY}"
    )


@asset(partitions_def=sources_partitions_def, deps=[nabu_orgs_release])
def nabu_orgs_prefix(context: AssetExecutionContext):
    """Move the orgs nq file(s) into the graphdb"""
    NabuContainer("orgs", context.partition_key).run(
        f"prefix --prefix orgs/{context.partition_key} --repository {DATAGRAPH_REPOSITORY}"
    )


@asset(
    partitions_def=sources_partitions_def,
    deps=[nabu_orgs_prefix, nabu_sync],
)
def finished_individual_crawl(context: AssetExecutionContext):
    """Dummy asset signifying the geoconnex crawl is completed once the orgs and prov nq files are in the graphdb and the graph is synced with the s3 bucket"""
    pass
