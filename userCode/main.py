import asyncio
from datetime import datetime
import os
import platform
import shutil
import subprocess
from typing import Optional
import zipfile
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    AssetSelection,
    RunRequest,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    OpExecutionContext,
    ScheduleEvaluationContext,
    asset,
    asset_check,
    define_asset_job,
    get_dagster_logger,
    load_asset_checks_from_current_module,
    load_assets_from_current_module,
    schedule,
    materialize,
)
import docker
import dagster_slack
import docker.errors
from threading import Thread
import requests
import yaml

from userCode.lib.lakefsUtils import merge_into_main

from .lib.classes import S3, FileTransferer
from .lib.utils import (
    all_dependencies_materialized,
    remove_non_alphanumeric,
    run_scheduler_docker_image,
    slack_error_fn,
    template_gleaner_or_nabu,
    template_rclone,
)
from urllib.parse import urlparse
from .lib.dagster_helpers import filter_partitions, sources_partitions_def
from .lib.env import (
    GLEANER_GRAPH_URL,
    GLEANER_HEADLESS_ENDPOINT,
    REMOTE_GLEANER_SITEMAP,
    GLEANERIO_DATAGRAPH_ENDPOINT,
    GLEANER_IMAGE,
    NABU_IMAGE,
    GLEANERIO_PROVGRAPH_ENDPOINT,
    RUNNING_AS_TEST_OR_DEV,
    strict_env,
)


@asset
def nabu_config():
    """The nabuconfig.yaml used for nabu"""
    get_dagster_logger().info("Creating nabu config")
    input_file = os.path.join(
        os.path.dirname(__file__), "templates", "nabuconfig.yaml.j2"
    )
    templated_data = template_gleaner_or_nabu(input_file)
    conf = yaml.safe_load(templated_data)
    encoded_as_bytes = yaml.safe_dump(conf).encode()
    # put configs in s3 for introspection and persistence if we need to run gleaner locally
    s3_client = S3()
    s3_client.load(
        encoded_as_bytes,
        "configs/nabuconfig.yaml",
    )
    with open("/tmp/geoconnex/nabuconfig.yaml", "w") as f:
        f.write(templated_data)


def ensure_local_bin_in_path():
    """Ensure ~/.local/bin is in the PATH."""
    local_bin = os.path.expanduser("~/.local/bin")
    if local_bin not in os.environ["PATH"].split(os.pathsep):
        os.environ["PATH"] += os.pathsep + local_bin
    return local_bin


@asset
def rclone_binary():
    """Download the rclone binary to a user-writable location in the PATH."""
    local_bin = ensure_local_bin_in_path()
    os.makedirs(local_bin, exist_ok=True)

    # Check if rclone is already installed in ~/.local/bin
    rclone_path = os.path.join(local_bin, "rclone")
    if os.path.isfile(rclone_path):
        print(f"Rclone is already installed at {rclone_path}.")
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


@asset(deps=[rclone_binary])
def rclone_config() -> str:
    """Create the rclone config by templating the rclone.conf.j2 template"""
    get_dagster_logger().info("Creating rclone config")
    input_file = os.path.join(os.path.dirname(__file__), "templates", "rclone.conf.j2")
    templated_conf: str = template_rclone(input_file)
    return templated_conf


@asset
def gleaner_config(context: AssetExecutionContext):
    """The gleanerconfig.yaml used for gleaner"""
    get_dagster_logger().info("Creating gleaner config")
    input_file = os.path.join(
        os.path.dirname(__file__), "templates", "gleanerconfig.yaml.j2"
    )

    # Fill in the config with the common minio configuration
    templated_base: dict = yaml.safe_load(template_gleaner_or_nabu(input_file))

    r = requests.get(REMOTE_GLEANER_SITEMAP)
    xml = r.text
    sitemapTags = BeautifulSoup(xml, features="xml").find_all("sitemap")
    Lines: list[str] = [sitemap.findNext("loc").text for sitemap in sitemapTags]

    sources = []
    names: set[str] = set()

    assert (
        len(Lines) > 0
    ), f"No sitemaps found in sitemap index {REMOTE_GLEANER_SITEMAP}"

    for line in Lines:
        basename = REMOTE_GLEANER_SITEMAP.removesuffix(".xml")
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
                f"Found duplicate name '{name}' in line '{line}' in sitemap {REMOTE_GLEANER_SITEMAP}. Skipping adding it again"
            )
            continue

        parsed_url = urlparse(REMOTE_GLEANER_SITEMAP)
        protocol, hostname = parsed_url.scheme, parsed_url.netloc
        data = {
            "sourcetype": "sitemap",
            "name": name,
            "url": line.strip(),
            # Headless should be false by default since most sites don't use it.
            # If gleaner cannot extract JSON-LD it will fallback to headless mode
            "headless": "false",
            "pid": "https://gleaner.io/genid/geoconnex",
            "propername": name,
            "domain": f"{protocol}://{hostname}",
            "active": "true",
        }
        names.add(name)
        sources.append(data)

    get_dagster_logger().info(f"Found {len(sources)} sources in the sitemap")

    filter_partitions(context.instance, "sources_partitions_def", names)

    # Each source is a partition that can be crawled independently
    context.instance.add_dynamic_partitions(
        partitions_def_name="sources_partitions_def", partition_keys=list(names)
    )

    templated_base["sources"] = sources

    get_dagster_logger().info(
        f"Generated the following gleaner config: {yaml.dump(templated_base)}"
    )

    # put configs in s3 for introspection and persistence if we need to run gleaner locally
    encoded_as_bytes = yaml.dump(templated_base).encode()
    s3_client = S3()
    s3_client.load(encoded_as_bytes, "configs/gleanerconfig.yaml")
    with open("/tmp/geoconnex/gleanerconfig.yaml", "w") as f:
        f.write(yaml.dump(templated_base))


@asset_check(asset=gleaner_config)
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


@asset()
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

    url = GLEANER_HEADLESS_ENDPOINT
    if RUNNING_AS_TEST_OR_DEV():
        portNumber = GLEANER_HEADLESS_ENDPOINT.removeprefix("http://").split(":")[1]
        url = f"http://localhost:{portNumber}"

    # the Host header needs to be set for Chromium due to an upstream security requirement
    result = requests.get(url, timeout=TWO_SECONDS)
    return AssetCheckResult(
        passed=result.status_code == 200,
        metadata={
            "status_code": result.status_code,
            "text": result.text,
            "endpoint": GLEANER_HEADLESS_ENDPOINT,
        },
    )


@asset(
    partitions_def=sources_partitions_def,
    deps=[docker_client_environment, gleaner_config, nabu_config],
)
def gleaner(context: OpExecutionContext):
    """Get the jsonld for each site in the gleaner config"""
    source = context.partition_key
    ARGS = ["--cfg", "gleanerconfig.yaml", "--source", source, "--rude"]

    run_scheduler_docker_image(
        source,
        GLEANER_IMAGE,
        ARGS,
        "gleaner",
        volumeMapping=["/tmp/geoconnex/gleanerconfig.yaml:/app/gleanerconfig.yaml"],
    )


@asset(partitions_def=sources_partitions_def, deps=[gleaner])
def nabu_release(context: OpExecutionContext):
    """Construct an nq file from all of the jsonld produced by gleaner"""
    source = context.partition_key
    ARGS = [
        "release",
        "--cfg",
        "nabuconfig.yaml",
        "--prefix",
        "summoned/" + source,
    ]
    run_scheduler_docker_image(
        source,
        NABU_IMAGE,
        ARGS,
        "release",
        volumeMapping=["/tmp/geoconnex/nabuconfig.yaml:/app/nabuconfig.yaml"],
    )


@asset(partitions_def=sources_partitions_def, deps=[nabu_release])
def nabu_object(context: OpExecutionContext):
    """Take the nq file from s3 and use the sparql API to upload it into the graph"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        "nabuconfig.yaml",
        "object",
        f"graphs/latest/{source}_release.nq",
        "--repository",
        GLEANERIO_DATAGRAPH_ENDPOINT,
    ]
    run_scheduler_docker_image(
        source,
        NABU_IMAGE,
        ARGS,
        "object",
        volumeMapping=["/tmp/geoconnex/nabuconfig.yaml:/app/nabuconfig.yaml"],
    )


@asset(partitions_def=sources_partitions_def, deps=[nabu_object])
def nabu_prune(context: OpExecutionContext):
    """Synchronize the graph with s3 by adding/removing from the graph"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        "nabuconfig.yaml",
        "prune",
        "--prefix",
        "summoned/" + source,
        "--repository",
        GLEANERIO_DATAGRAPH_ENDPOINT,
    ]
    run_scheduler_docker_image(
        source,
        NABU_IMAGE,
        ARGS,
        "prune",
        volumeMapping=["/tmp/geoconnex/nabuconfig.yaml:/app/nabuconfig.yaml"],
    )


@asset(partitions_def=sources_partitions_def, deps=[gleaner])
def nabu_prov_release(context):
    """Construct an nq file from all of the jsonld prov produced by gleaner.
    Used for tracing data lineage"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        "nabuconfig.yaml",
        "release",
        "--prefix",
        "prov/" + source,
    ]
    run_scheduler_docker_image(
        source,
        NABU_IMAGE,
        ARGS,
        "prov-release",
        volumeMapping=["/tmp/geoconnex/nabuconfig.yaml:/app/nabuconfig.yaml"],
    )


@asset(partitions_def=sources_partitions_def, deps=[nabu_prov_release])
def nabu_prov_clear(context: OpExecutionContext):
    """Clears the prov graph before putting the new nq in"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        "nabuconfig.yaml",
        "clear",
        "--repository",
        GLEANERIO_PROVGRAPH_ENDPOINT,
    ]
    run_scheduler_docker_image(
        source,
        NABU_IMAGE,
        ARGS,
        "prov-clear",
        volumeMapping=["/tmp/geoconnex/nabuconfig.yaml:/app/nabuconfig.yaml"],
    )


@asset(partitions_def=sources_partitions_def, deps=[nabu_prov_clear])
def nabu_prov_object(context):
    """Take the nq file from s3 and use the sparql API to upload it into the prov graph repository"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        "nabuconfig.yaml",
        "object",
        f"graphs/latest/{source}_prov.nq",
        "--repository",
        GLEANERIO_PROVGRAPH_ENDPOINT,
    ]
    run_scheduler_docker_image(
        source,
        NABU_IMAGE,
        ARGS,
        "prov-object",
        volumeMapping=["/tmp/geoconnex/nabuconfig.yaml:/app/nabuconfig.yaml"],
    )


@asset(partitions_def=sources_partitions_def, deps=[gleaner])
def nabu_orgs_release(context: OpExecutionContext):
    """Construct an nq file for the metadata of all the organizations. Their data is not included in this step.
    This is just flat metadata"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        "nabuconfig.yaml",
        "release",
        "--prefix",
        "orgs",
        "--repository",
        GLEANERIO_DATAGRAPH_ENDPOINT,
    ]
    run_scheduler_docker_image(
        source,
        NABU_IMAGE,
        ARGS,
        "orgs-release",
        volumeMapping=["/tmp/geoconnex/nabuconfig.yaml:/app/nabuconfig.yaml"],
    )


@asset(partitions_def=sources_partitions_def, deps=[nabu_orgs_release])
def nabu_orgs(context: OpExecutionContext):
    """Move the orgs nq file(s) into the graphdb"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        "nabuconfig.yaml",
        "prefix",
        "--prefix",
        "orgs",
        "--repository",
        GLEANERIO_DATAGRAPH_ENDPOINT,
    ]
    run_scheduler_docker_image(
        source,
        NABU_IMAGE,
        ARGS,
        "orgs",
        volumeMapping=["/tmp/geoconnex/nabuconfig.yaml:/app/nabuconfig.yaml"],
    )


@asset(
    partitions_def=sources_partitions_def,
    deps=[nabu_orgs, nabu_prune],
)
def finished_individual_crawl(context: OpExecutionContext):
    """Dummy asset signifying the geoconnex crawl is completed once the orgs and prov nq files are in the graphdb and the graph is synced with the s3 bucket"""
    pass


@asset(deps=[finished_individual_crawl])
def export_graph_as_nquads(context: OpExecutionContext) -> Optional[str]:
    """Export the graphdb to nquads"""

    if not all_dependencies_materialized(context, "finished_individual_crawl"):
        return

    base_url = (
        GLEANER_GRAPH_URL if not RUNNING_AS_TEST_OR_DEV() else "http://localhost:7200"
    )

    # Define the repository name and endpoint
    endpoint = (
        f"{base_url}/repositories/{GLEANERIO_DATAGRAPH_ENDPOINT}/statements?infer=false"
    )

    get_dagster_logger().info(
        f"Exporting graphdb to nquads; fetching data from {endpoint}"
    )
    # Download the nq export
    response = requests.get(
        endpoint,
        headers={
            "Accept": "application/n-quads",
        },
    )

    # Check if the request was successful
    if response.status_code == 200:
        # Save the response content to a file
        with open("outputfile.nq", "wb") as f:
            f.write(response.content)
        get_dagster_logger().info("Export of graphdb to nquads successful")
    else:
        raise RuntimeError(
            f"Export failed, status code: {response.status_code} with response {response.text}"
        )

    s3_client = S3()
    filename = f"backups/nquads_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}"
    s3_client.load(response.content, filename)
    return filename


@asset()
def nquads_to_renci(
    context: OpExecutionContext,
    rclone_config: str,
    export_graph_as_nquads: Optional[str],  # contains the path to the nquads
):
    """Upload the nquads to the renci bucket in lakefs"""
    if (
        not all_dependencies_materialized(context, "finished_individual_crawl")
        or not export_graph_as_nquads
    ):
        get_dagster_logger().warning(
            "Skipping rclone copy as all dependencies are not materialized"
        )
        return

    if RUNNING_AS_TEST_OR_DEV():
        get_dagster_logger().warning(
            "Skipping rclone copy as we are running in test mode"
        )
        return

    client = FileTransferer(rclone_config)
    client.copy_to_lakefs(
        destination_branch="develop",
        destination_filename="iow-dump.nq",
        path_to_file=export_graph_as_nquads,
    )
    merge_into_main(branch="develop")


harvest_job = define_asset_job(
    "harvest_source",
    description="harvest a source for the geoconnex graphdb",
    selection=AssetSelection.all(),
)


@schedule(
    cron_schedule="@weekly",
    job=harvest_job,
    default_status=DefaultScheduleStatus.STOPPED
    if RUNNING_AS_TEST_OR_DEV()
    else DefaultScheduleStatus.RUNNING,
)
def crawl_entire_graph_schedule(context: ScheduleEvaluationContext):
    get_dagster_logger().info("Schedule triggered.")

    get_dagster_logger().info("Deleting old partition status before new crawl")
    filter_partitions(context.instance, "sources_partitions_def", keys_to_keep=set())

    result = materialize([gleaner_config], instance=context.instance)
    if not result.success:
        raise Exception(f"Failed to materialize gleaner_config!: {result}")

    partition_keys = context.instance.get_dynamic_partitions("sources_partitions_def")
    get_dagster_logger().info(f"Found partition keys: {partition_keys}")

    if not partition_keys:
        raise Exception("No partition keys found after materializing gleaner_config!")

    for partition_key in partition_keys:
        yield RunRequest(
            job_name="harvest_source",
            run_key="havest_weekly",
            partition_key=partition_key,
            tags={"run_type": "harvest_weekly"},
        )


# expose all the code needed for our dagster repo
definitions = Definitions(
    assets=load_assets_from_current_module(),
    schedules=[crawl_entire_graph_schedule],
    asset_checks=load_asset_checks_from_current_module(),
    sensors=[
        dagster_slack.make_slack_on_run_failure_sensor(
            channel="#cgs-iow-bots",
            slack_token=strict_env("DAGSTER_SLACK_TOKEN"),
            text_fn=slack_error_fn,
            default_status=DefaultSensorStatus.STOPPED
            if RUNNING_AS_TEST_OR_DEV()
            else DefaultSensorStatus.RUNNING,
            monitor_all_code_locations=True,
        )
    ],
    # Commented out but can uncomment if we want to send other slack msgs
    # resources={
    #     "slack": dagster_slack.SlackResource(token=strict_env("DAGSTER_SLACK_TOKEN")),
    # },
)
