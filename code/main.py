from datetime import datetime
from dagster import (
    RunRequest,
    StaticPartitionsDefinition,
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
    run_scheduler_docker_image,
    s3loader,
    slack_error_fn,
)

from lib.env import (
    GLEANER_CONFIG_PATH,
    GLEANER_GRAPH_URL,
    GLEANERIO_DATAGRAPH_ENDPOINT,
    GLEANERIO_GLEANER_CONFIG_PATH,
    GLEANERIO_GLEANER_IMAGE,
    GLEANERIO_NABU_CONFIG_PATH,
    GLEANERIO_NABU_IMAGE,
    GLEANERIO_PROVGRAPH_ENDPOINT,
    strict_env,
)


def get_gleaner_config_sources() -> list[GleanerSource]:
    """Given a config, return the jobs that will need to be run to perform a full geoconnex crawl"""
    with open(GLEANER_CONFIG_PATH) as f:
        config = yaml.safe_load(f)
        all_sources = [site for site in config["sources"]]
        assert len(all_sources) > 0
        return all_sources


names = [config["name"] for config in get_gleaner_config_sources()]
sources_partitions_def = StaticPartitionsDefinition(names)


@asset
def pull_docker_images():
    """Set up dagster by pulling both the gleaner and nabu images we will later use"""
    get_dagster_logger().info("Getting docker client and pulling images: ")
    client = docker.DockerClient(version="1.43")
    client.images.pull(GLEANERIO_GLEANER_IMAGE)
    client.images.pull(GLEANERIO_NABU_IMAGE)


@asset(partitions_def=sources_partitions_def, deps=[pull_docker_images])
def gleaner(context: OpExecutionContext):
    """Get the jsonld for each site in the gleaner config"""
    source = context.partition_key
    ARGS = ["--cfg", GLEANERIO_GLEANER_CONFIG_PATH, "-source", source, "--rude"]
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
        GLEANERIO_NABU_CONFIG_PATH,
        "--prefix",
        "summoned/" + source,
    ]
    returned_value = run_scheduler_docker_image(
        context, source, GLEANERIO_NABU_IMAGE, ARGS, "release"
    )
    get_dagster_logger().info(f"nabu release returned value '{returned_value}'")


@asset(partitions_def=sources_partitions_def, deps=[nabu_release])
def nabu_object(context: OpExecutionContext):
    """Take the nq file from s3 and use the sparql API to upload it into the graph"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        GLEANERIO_NABU_CONFIG_PATH,
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
def nabu_prune(context: OpExecutionContext):
    """Synchronize the graph with s3 by adding/removing from the graph"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        GLEANERIO_NABU_CONFIG_PATH,
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
def nabu_prov_release(context):
    """Construct an nq file from all of the jsonld prov produced by gleaner.
    Used for tracing data lineage"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        GLEANERIO_NABU_CONFIG_PATH,
        "release",
        "--prefix",
        "prov/" + source,
    ]
    returned_value = run_scheduler_docker_image(
        context, source, GLEANERIO_NABU_IMAGE, ARGS, "prov-release"
    )
    get_dagster_logger().info(f"nabu prov-release returned value '{returned_value}'")


@asset(partitions_def=sources_partitions_def, deps=[nabu_prov_release])
def nabu_prov_clear(context: OpExecutionContext):
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
def nabu_prov_object(context):
    """Take the nq file from s3 and use the sparql API to upload it into the graph"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        GLEANERIO_NABU_CONFIG_PATH,
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
def nabu_orgs_release(context: OpExecutionContext):
    """Construct an nq file for all the organizations. Their data is not included in this step.
    This is just flat metadata"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        GLEANERIO_NABU_CONFIG_PATH,
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
def nabu_orgs(context: OpExecutionContext):
    """Move the orgs nq file(s) into the graphdb"""
    source = context.partition_key
    ARGS = [
        "--cfg",
        GLEANERIO_NABU_CONFIG_PATH,
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
    pull_docker_images,
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
    tags={"dagster/max_runtime": THREE_MIN}
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
