from datetime import datetime
from email.mime import base
from typing import List
from bs4 import BeautifulSoup
from dagster import (
    DynamicPartitionsDefinition,
    RunRequest,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    OpExecutionContext,
    asset,
    define_asset_job,
    get_dagster_logger,
    schedule,
)
import docker
import dagster_slack
import requests
import yaml
from lib.types import GleanerSource
from lib.utils import (
    remove_non_alphanumeric,
    run_scheduler_docker_image,
    s3loader,
    s3reader,
    slack_error_fn,
    template_config,
)

from lib.env import (
    GLEANER_GRAPH_URL,
    GLEANERIO_DATAGRAPH_ENDPOINT,
    GLEANERIO_GLEANER_IMAGE,
    GLEANERIO_NABU_CONFIG_PATH,
    GLEANERIO_NABU_IMAGE,
    GLEANERIO_PROVGRAPH_ENDPOINT,
    strict_env,
)
sources_partitions_def = DynamicPartitionsDefinition(name="sources_partitions_def")

@asset
def nabu_config():
    """The nabuconfig.yaml used for nabu"""
    get_dagster_logger().info("Creating nabu config")
    pass

@asset
def gleaner_config():
    """The gleanerconfig.yaml used for gleaner"""
    get_dagster_logger().info("Creating gleaner config")
    input_file = "/opt/dagster/app/templates/gleanerconfig.yaml.j2"

    # Fill in the config with the common minio configuration
    templated_base = template_config(input_file)

    sitemap_url = "https://geoconnex.us/sitemap.xml"
    # Parse the sitemap index for the referenced sitemaps for a config file
    r = requests.get(sitemap_url)
    xml = r.text
    sitemapTags = BeautifulSoup(xml, features="xml").find_all("sitemap")
    Lines: list[str] = [sitemap.findNext("loc").text for sitemap in sitemapTags]

    sources = []
    names = set()
    for line in Lines:
        basename = sitemap_url.removesuffix(".xml")
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

        data = {
            "sourcetype": "sitemap",
            "name": name,
            "url": line.strip(),
            "headless": "false",
            "pid": "https://gleaner.io/genid/geoconnex",
            "propername": name,
            "domain": "https://geoconnex.us",
            "active": "true",
        }
        names.add(name)
        sources.append(data)

    # Combine base data with the new sources
    if isinstance(templated_base, dict):
        templated_base["sources"] = sources
    else:
        templated_base = {"sources": sources}

    encoded_as_bytes = yaml.dump(templated_base).encode()
    s3loader(encoded_as_bytes, "/configs/gleanerconfig.yaml")


@asset
def pull_docker_images():
    """Set up dagster by pulling both the gleaner and nabu images we will later use"""
    get_dagster_logger().info("Getting docker client and pulling images: ")
    client = docker.DockerClient(version="1.43")
    client.images.pull(GLEANERIO_GLEANER_IMAGE)
    client.images.pull(GLEANERIO_NABU_IMAGE)


@asset(deps=[nabu_config, gleaner_config])
def dagster_partitions():
    """Partition the sources"""
    config = s3reader("configs/gleanerconfig.yaml")
    config = yaml.safe_load(config)
    # config = yaml.safe_load(gleaner_config)
    all_sources: List[GleanerSource] = [site for site in config["sources"]]
    assert len(all_sources) > 0

    for item in all_sources:
        sources_partitions_def.build_add_request([item["name"]])
        get_dagster_logger().info(f"Added partition {item['name']}")


@asset(partitions_def=sources_partitions_def, deps=[pull_docker_images, dagster_partitions])
def gleaner(context: OpExecutionContext, gleaner_config: str):
    """Get the jsonld for each site in the gleaner config"""
    # with the gleaner config to disk as a yaml file
    with open("gleanerconfig.yaml", "w") as f:
        yaml.dump(gleaner_config, f)

    source = context.partition_key
    ARGS = ["--cfg", "gleanerconfig.yaml", "-source", source, "--rude"]
    returned_value = run_scheduler_docker_image(
        context, source, gleaner_config, ARGS, "gleaner"
    )
    get_dagster_logger().info(f"Gleaner returned value: '{returned_value}'")


@asset(partitions_def=sources_partitions_def, deps=[gleaner])
def nabu_release(context: OpExecutionContext):
    """Construct an nq file from all of the jsonld produced by gleaner"""
    source = context.partition_key
    ARGS = [
        "release",
        "--cfg",
        GLEANERIO_NABU_CONFIG_PATH,
        "--prefix",
        "summoned/" + source,
    ]
    returned_value = run_scheduler_docker_image(
        context, source, GLEANERIO_NABU_IMAGE, ARGS, "release"
    )
    get_dagster_logger().info(f"nabu release returned value '{returned_value}'")


@asset(partitions_def=sources_partitions_def, deps=[nabu_release])
def nabu_object(context: OpExecutionContext, nabu_config: str):
    """Take the nq file from s3 and use the sparql API to upload it into the graph"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        nabu_config,
        "object",
        f"/graphs/latest/{source}_release.nq",
        "--endpoint",
        GLEANERIO_DATAGRAPH_ENDPOINT,
    ]
    returned_value = run_scheduler_docker_image(
        context, source, GLEANERIO_NABU_IMAGE, ARGS, "object"
    )
    get_dagster_logger().info(f"nabu release returned value '{returned_value}'")


@asset(partitions_def=sources_partitions_def, deps=[nabu_object])
def nabu_prune(context: OpExecutionContext, nabu_config: str):
    """Synchronize the graph with s3 by adding/removing from the graph"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        nabu_config,
        "prune",
        "--prefix",
        "summoned/" + source,
        "--endpoint",
        GLEANERIO_DATAGRAPH_ENDPOINT,
    ]
    returned_value = run_scheduler_docker_image(
        context, source, GLEANERIO_NABU_IMAGE, ARGS, "prune"
    )
    get_dagster_logger().info(f"nabu prune returned value '{returned_value}'")


@asset(partitions_def=sources_partitions_def, deps=[gleaner])
def nabu_prov_release(context, nabu_config: str):
    """Construct an nq file from all of the jsonld prov produced by gleaner.
    Used for tracing data lineage"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        nabu_config,
        "release",
        "--prefix",
        "prov/" + source,
    ]
    returned_value = run_scheduler_docker_image(
        context, source, GLEANERIO_NABU_IMAGE, ARGS, "prov-release"
    )
    get_dagster_logger().info(f"nabu prov-release returned value '{returned_value}'")


@asset(partitions_def=sources_partitions_def, deps=[nabu_prov_release])
def nabu_prov_clear(context: OpExecutionContext, nabu_config: str):
    """Clears the prov graph before putting the new nq in"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        GLEANERIO_NABU_CONFIG_PATH,
        "clear",
        "--endpoint",
        GLEANERIO_PROVGRAPH_ENDPOINT,
    ]
    returned_value = run_scheduler_docker_image(
        context, source, GLEANERIO_NABU_IMAGE, ARGS, "prov-clear"
    )
    get_dagster_logger().info(f"nabu prov-clear returned value '{returned_value}'")


@asset(partitions_def=sources_partitions_def, deps=[nabu_prov_clear])
def nabu_prov_object(context, nabu_config: str):
    """Take the nq file from s3 and use the sparql API to upload it into the prov graph repository"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        nabu_config,
        "object",
        f"/graphs/latest/{source}_prov.nq",
        "--endpoint",
        GLEANERIO_PROVGRAPH_ENDPOINT,
    ]
    returned_value = run_scheduler_docker_image(
        context, source, GLEANERIO_NABU_IMAGE, ARGS, "prov-object"
    )
    get_dagster_logger().info(f"nabu prov-object returned value '{returned_value}'")


@asset(partitions_def=sources_partitions_def, deps=[gleaner])
def nabu_orgs_release(context: OpExecutionContext, nabu_config: str):
    """Construct an nq file for the metadata of all the organizations. Their data is not included in this step.
    This is just flat metadata"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        nabu_config,
        "release",
        "--prefix",
        "orgs",
        "--endpoint",
        GLEANERIO_DATAGRAPH_ENDPOINT,
    ]
    returned_value = run_scheduler_docker_image(
        context, source, GLEANERIO_NABU_IMAGE, ARGS, "orgs-release"
    )
    get_dagster_logger().info(f"nabu orgs-release returned value '{returned_value}'")


@asset(partitions_def=sources_partitions_def, deps=[nabu_orgs_release])
def nabu_orgs(context: OpExecutionContext, nabu_config: str):
    """Move the orgs nq file(s) into the graphdb"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        nabu_config,
        "prefix",
        "--prefix",
        "orgs",
        "--endpoint",
        GLEANERIO_DATAGRAPH_ENDPOINT,
    ]
    returned_value = run_scheduler_docker_image(
        context, source, GLEANERIO_NABU_IMAGE, ARGS, "orgs"
    )
    get_dagster_logger().info(f"nabu orgs returned value '{returned_value}'")


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

    s3loader(
        response.content,
        f"backups/nquads_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}",
    )


all_assets = [
    # we need this variable since AssetSelection.all() doesn't appear to work
    pull_docker_images,
    gleaner_config,
    nabu_config,
    dagster_partitions,
    gleaner,
    nabu_object,
    nabu_release,
    nabu_prune,
    nabu_prov_release,
    nabu_prov_clear,
    nabu_prov_object,
    nabu_orgs_release,
    nabu_orgs,
    finished_individual_crawl,
    export_graph_as_nquads,
]

THREE_MIN = 60 * 3

harvest_job = define_asset_job(
    "harvest_source",
    description="harvest a source for the geoconnex graphdb",
    selection=all_assets,
    tags={"dagster/max_runtime": THREE_MIN},
)


@schedule(
    cron_schedule="@daily",
    job=harvest_job,
    default_status=DefaultScheduleStatus.STOPPED,
)
def geoconnex_schedule():
    for partition_key in sources_partitions_def.get_partition_keys():
        yield RunRequest(partition_key=partition_key)


# expose all the code needed for our dagster repo
definitions = Definitions(
    assets=all_assets,
    schedules=[geoconnex_schedule],
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
