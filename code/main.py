import asyncio
from datetime import datetime
from typing import Tuple
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    AssetSelection,
    DynamicPartitionsDefinition,
    RunRequest,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    OpExecutionContext,
    asset,
    asset_check,
    define_asset_job,
    get_dagster_logger,
    load_asset_checks_from_current_module,
    load_assets_from_current_module,
    schedule,
)
import docker
import dagster_slack
import requests
import yaml
from lib.classes import S3
from lib.utils import (
    remove_non_alphanumeric,
    run_scheduler_docker_image,
    slack_error_fn,
    template_config,
)
from urllib.parse import urlparse

from lib.env import (
    GLEANER_GRAPH_URL,
    GLEANER_HEADLESS_ENDPOINT,
    REMOTE_GLEANER_SITEMAP,
    GLEANERIO_DATAGRAPH_ENDPOINT,
    GLEANERIO_GLEANER_IMAGE,
    GLEANERIO_NABU_IMAGE,
    GLEANERIO_PROVGRAPH_ENDPOINT,
    strict_env,
)

sources_partitions_def = DynamicPartitionsDefinition(name="sources_partitions_def")


@asset
def nabu_config():
    """The nabuconfig.yaml used for nabu"""
    get_dagster_logger().info("Creating nabu config")
    templated_data = template_config("/opt/dagster/app/templates/nabuconfig.yaml.j2")
    conf = yaml.safe_load(templated_data)
    encoded_as_bytes = yaml.safe_dump(conf).encode()
    # put configs in s3 for introspection and persistence if we need to run gleaner locally
    s3_client = S3()
    s3_client.load(
        encoded_as_bytes,
        "configs/nabuconfig.yaml",
    )


@asset
def gleaner_config(context: AssetExecutionContext):
    """The gleanerconfig.yaml used for gleaner"""
    get_dagster_logger().info("Creating gleaner config")
    input_file = "/opt/dagster/app/templates/gleanerconfig.yaml.j2"

    # Fill in the config with the common minio configuration
    templated_base = yaml.safe_load(template_config(input_file))

    r = requests.get(REMOTE_GLEANER_SITEMAP)
    xml = r.text
    sitemapTags = BeautifulSoup(xml, features="xml").find_all("sitemap")
    Lines: list[str] = [sitemap.findNext("loc").text for sitemap in sitemapTags]

    sources = []
    names = set()
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
            print(f"Warning! Skipping duplicate name {name}")
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

    # Each source is a partition that can be crawled independently
    context.instance.add_dynamic_partitions(
        partitions_def_name="sources_partitions_def", partition_keys=list(names)
    )

    templated_base["sources"] = sources

    # put configs in s3 for introspection and persistence if we need to run gleaner locally
    encoded_as_bytes = yaml.dump(templated_base).encode()
    s3_client = S3()
    s3_client.load(encoded_as_bytes, "configs/gleanerconfig.yaml")


@asset_check(asset=gleaner_config)
def gleaner_links_are_valid():
    """Check if all the links in the gleaner config are valid and validate all 'loc' tags in the XML at each UR"""
    s3_client = S3()
    config = s3_client.read("configs/gleanerconfig.yaml")
    yaml_config = yaml.safe_load(config)

    dead_links: list[dict[str, Tuple[int, str]]] = []

    async def validate_url(url: str):
        async with ClientSession() as session:
            resp = await session.get(url)

            if resp.status != 200:
                content = await resp.text()
                get_dagster_logger().error(
                    f"URL {url} returned status code {resp.status} with content: {content}"
                )
                dead_links.append({url: (resp.status, content)})

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


@asset(deps=[gleaner_config, nabu_config])
def docker_client_environment():
    """Set up dagster by pulling both the gleaner and nabu images and moving the config files into docker configs"""
    get_dagster_logger().info("Getting docker client and pulling images: ")
    client = docker.DockerClient(version="1.43")
    client.images.pull(GLEANERIO_GLEANER_IMAGE)
    client.images.pull(GLEANERIO_NABU_IMAGE)
    # we create configs as docker config objects so
    # we can more easily reuse them and not need to worry about
    # navigating / mounting file systems for local config access
    api_client = docker.APIClient()

    try:
        gleanerconfig = client.configs.list(filters={"name": ["gleaner"]})
        nabuconfig = client.configs.list(filters={"name": ["nabu"]})
        if gleanerconfig:
            api_client.remove_config(gleanerconfig[0].id)
        if nabuconfig:
            api_client.remove_config(nabuconfig[0].id)
    except IndexError as e:
        get_dagster_logger().info(
            f"No configs found to remove during docker client environment creation: {e}"
        )

    s3_client = S3()
    client.configs.create(name="nabu", data=s3_client.read("configs/nabuconfig.yaml"))
    client.configs.create(
        name="gleaner", data=s3_client.read("configs/gleanerconfig.yaml")
    )


@asset_check(asset=docker_client_environment)
def can_contact_headless():
    """Check that we can contact the headless server"""
    TWO_SECONDS = 2
    # the Host header needs to be set for Chromium due to an upstream security requirement
    result = requests.get(
        GLEANER_HEADLESS_ENDPOINT, timeout=TWO_SECONDS, headers={"Host": "localhost"}
    )
    return AssetCheckResult(
        passed=result.status_code == 200,
        metadata={
            "status_code": result.status_code,
            "text": result.text,
            "endpoint": GLEANER_HEADLESS_ENDPOINT,
        },
    )


@asset(partitions_def=sources_partitions_def, deps=[docker_client_environment])
def gleaner(context: OpExecutionContext):
    """Get the jsonld for each site in the gleaner config"""
    source = context.partition_key
    ARGS = ["--cfg", "gleanerconfig.yaml", "-source", source, "--rude"]
    returned_value = run_scheduler_docker_image(
        context, source, GLEANERIO_GLEANER_IMAGE, ARGS, "gleaner"
    )
    get_dagster_logger().info(f"Gleaner returned value: '{returned_value}'")


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
    run_scheduler_docker_image(context, source, GLEANERIO_NABU_IMAGE, ARGS, "release")


@asset(partitions_def=sources_partitions_def, deps=[nabu_release])
def nabu_object(context: OpExecutionContext):
    """Take the nq file from s3 and use the sparql API to upload it into the graph"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        "nabuconfig.yaml",
        "object",
        f"graphs/latest/{source}_release.nq",
        "--endpoint",
        GLEANERIO_DATAGRAPH_ENDPOINT,
    ]
    run_scheduler_docker_image(context, source, GLEANERIO_NABU_IMAGE, ARGS, "object")


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
        "--endpoint",
        GLEANERIO_DATAGRAPH_ENDPOINT,
    ]
    run_scheduler_docker_image(context, source, GLEANERIO_NABU_IMAGE, ARGS, "prune")


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
        context, source, GLEANERIO_NABU_IMAGE, ARGS, "prov-release"
    )


@asset(partitions_def=sources_partitions_def, deps=[nabu_prov_release])
def nabu_prov_clear(context: OpExecutionContext):
    """Clears the prov graph before putting the new nq in"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        "nabuconfig.yaml",
        "clear",
        "--endpoint",
        GLEANERIO_PROVGRAPH_ENDPOINT,
    ]
    run_scheduler_docker_image(
        context, source, GLEANERIO_NABU_IMAGE, ARGS, "prov-clear"
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
        "--endpoint",
        GLEANERIO_PROVGRAPH_ENDPOINT,
    ]
    run_scheduler_docker_image(
        context, source, GLEANERIO_NABU_IMAGE, ARGS, "prov-object"
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
        "--endpoint",
        GLEANERIO_DATAGRAPH_ENDPOINT,
    ]
    run_scheduler_docker_image(
        context, source, GLEANERIO_NABU_IMAGE, ARGS, "orgs-release"
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
        "--endpoint",
        GLEANERIO_DATAGRAPH_ENDPOINT,
    ]
    run_scheduler_docker_image(context, source, GLEANERIO_NABU_IMAGE, ARGS, "orgs")


@asset(
    partitions_def=sources_partitions_def,
    deps=[nabu_orgs, nabu_prov_object, nabu_prune],
)
def finished_individual_crawl(context: OpExecutionContext):
    """Dummy asset signifying the geoconnex crawl is completed once the orgs and prov nq files are in the graphdb and the graph is synced with the s3 bucket"""
    pass


@asset(deps=[finished_individual_crawl])
def export_graph_as_nquads():
    """Export the graphdb to nquads"""

    # Define the repository name and endpoint
    endpoint = f"{GLEANER_GRAPH_URL}/repositories/{GLEANERIO_DATAGRAPH_ENDPOINT}/statements?infer=false"

    headers = {
        "Accept": "application/n-quads",
    }

    # Send the POST request to export the data
    response = requests.get(endpoint, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Save the response content to a file
        with open("outputfile.nq", "wb") as f:
            f.write(response.content)
        get_dagster_logger().info("Export of graphdb to nquads successful")
    else:
        get_dagster_logger().error(f"Response: {response.text}")
        raise RuntimeError(f"Export failed, status code: {response.status_code}")

    s3_client = S3()
    s3_client.load(
        response.content,
        f"backups/nquads_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}",
    )


THREE_MIN = 60 * 3

harvest_job = define_asset_job(
    "harvest_source",
    description="harvest a source for the geoconnex graphdb",
    selection=AssetSelection.all(),
    # special tag for dagster that limits max runtime (+ the tick interval)
    tags={"dagster/max_runtime": THREE_MIN},
)


@schedule(
    cron_schedule="@daily",
    job=harvest_job,
    default_status=DefaultScheduleStatus.STOPPED,
)
def crawl_entire_graph_schedule():
    for partition_key in sources_partitions_def.get_partition_keys():
        yield RunRequest(partition_key=partition_key)


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
            default_status=DefaultSensorStatus.RUNNING,
            monitor_all_code_locations=True,
            monitor_all_repositories=True,
            monitored_jobs=[harvest_job],
        )
    ],
    # Commented out but can uncomment if we want to send other slack msgs
    # resources={
    #     "slack": dagster_slack.SlackResource(token=strict_env("DAGSTER_SLACK_TOKEN")),
    # },
)
