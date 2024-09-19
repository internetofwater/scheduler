from dagster import (
    DefaultSensorStatus,
    Definitions,
    In,
    JobDefinition,
    Nothing,
    OpExecutionContext,
    RunFailureSensorContext,
    ScheduleDefinition,
    asset,
    define_asset_job,
    get_dagster_logger,
    op,
    graph,
)
import docker
import dagster_slack

from lib.types import GleanerSource
from lib.utils import (
    run_scheduler_docker_image,
)
import yaml

from lib.env import (
    GLEANERIO_DATAGRAPH_ENDPOINT,
    GLEANERIO_GLEANER_CONFIG_PATH,
    GLEANERIO_GLEANER_IMAGE,
    GLEANERIO_NABU_CONFIG_PATH,
    GLEANERIO_NABU_IMAGE,
    GLEANERIO_PROVGRAPH_ENDPOINT,
    GLEANER_CONFIG_PATH,
    strict_env,
)


def strict_get_tag(context: OpExecutionContext, key: str) -> str:
    """Gets a tag and make sure it exists before running further jobs"""
    src = context.run_tags[key]
    if src is None:
        raise Exception(f"Missing run tag {key}")
    return src


@op
def pull_docker_images():
    """Set up dagster by pulling both the gleaner and nabu images we will later use"""
    get_dagster_logger().info("Getting docker client and pulling images: ")
    client = docker.DockerClient(version="1.43")
    client.images.pull(GLEANERIO_GLEANER_IMAGE)
    client.images.pull(GLEANERIO_NABU_IMAGE)


@op(ins={"start": In(Nothing)})
def gleaner(context: OpExecutionContext):
    """Get the jsonld for each site in the gleaner config"""
    source = strict_get_tag(context, "source")
    ARGS = ["--cfg", GLEANERIO_GLEANER_CONFIG_PATH, "-source", source, "--rude"]
    returned_value = run_scheduler_docker_image(
        context, source, GLEANERIO_GLEANER_IMAGE, ARGS, "gleaner"
    )
    get_dagster_logger().info(f"Gleaner returned value: '{returned_value}'")


@op(ins={"start": In(Nothing)})
def nabu_release(context: OpExecutionContext):
    """Construct an nq file from all of the jsonld produced by gleaner"""
    source = strict_get_tag(context, "source")
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


@op(ins={"start": In(Nothing)})
def nabu_object(context: OpExecutionContext):
    """Take the nq file from s3 and use the sparql API to upload it into the graph"""
    source = strict_get_tag(context, "source")
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


@op(ins={"start": In(Nothing)})
def nabu_prune(context: OpExecutionContext):
    """Synchronize the graph with s3 by adding/removing from the graph"""
    source = strict_get_tag(context, "source")
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


@op(ins={"start": In(Nothing)})
def nabu_prov_release(context):
    """Construct an nq file from all of the jsonld prov produced by gleaner.
    Used for tracing data lineage"""
    source = strict_get_tag(context, "source")
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


@op(ins={"start": In(Nothing)})
def nabu_prov_clear(context: OpExecutionContext):
    """Clears the prov graph before putting the new nq in"""
    source = strict_get_tag(context, "source")
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


@op(ins={"start": In(Nothing)})
def nabu_prov_object(context):
    """Take the nq file from s3 and use the sparql API to upload it into the graph"""
    source = strict_get_tag(context, "source")
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


@op(ins={"start": In(Nothing)})
def nabu_orgs_release(context: OpExecutionContext):
    """Construct an nq file for all the organizations. Their data is not included in this step.
    This is just flat metadata"""
    source = strict_get_tag(context, "source")
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


@op(ins={"start": In(Nothing)})
def nabu_orgs(context: OpExecutionContext):
    """Move the orgs nq file(s) into the graphdb"""
    source = strict_get_tag(context, "source")
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


@graph
def harvest():
    """
    Harvest all assets for a given source.
    All source specific info is passed via tags within the run context
    """

    setup = pull_docker_images()

    # Get the jsonld for each site in the gleaner config
    jsonld = gleaner(start=setup)

    ### data branch
    # construct the nq files from all the jsonld produced by gleaner
    release = nabu_release(start=jsonld)

    # take the nq file from the s3 and use the sparql api to upload the nq object into the graph
    upload = nabu_object(start=release)

    # synchronize the graph with the s3 bucket by pruning/adding to the graph
    nabu_prune(start=upload)

    ### prov branch
    # construct the nq files from all the jsonld produced by gleaner
    # however, use prov to trace the json and upload it in a special prov repo
    # not the main graph
    prov_release = nabu_prov_release(start=jsonld)
    clear = nabu_prov_clear(start=prov_release)
    nabu_prov_object(start=clear)

    ### org branch
    # construct the nq files from all the jsonld produced by gleaner
    # specification about the organizations that provided the jsonld.
    orgs_release = nabu_orgs_release(start=jsonld)
    # load the nq files into the graph
    nabu_orgs(start=orgs_release)


def generate_job(
    source: GleanerSource,
) ->JobDefinition:
    """Given a source from the gleaner config, generate a dagster job and schedule"""
    return harvest.to_job(
        name="harvest_" + source["name"],
        description=f"harvest all assets for {source['name']}",
        tags={"source": source["name"]},
    )

def get_gleaner_config_sources() -> list[GleanerSource]:
    """Given a config, return the jobs that will need to be run to perform a full geoconnex crawl"""
    with open(GLEANER_CONFIG_PATH) as f:
        config = yaml.safe_load(f)
        all_sources = [site for site in config["sources"]]
        assert len(all_sources) > 0
        return all_sources


sources = get_gleaner_config_sources()
individual_source_crawls: list[JobDefinition] = []
for sitemap in sources:
    individual_source_crawls.append(generate_job(sitemap))

def slack_error_fn(context: RunFailureSensorContext) -> str:
    get_dagster_logger().info("Sending notification to slack")
    # The make_slack_on_run_failure_sensor automatically sends the job
    # id and name so you can just send the error. We don't need other data in the string
    return f"Error: {context.failure_event.message}"



@asset(deps=[job.name for job in individual_source_crawls])
def geoconnex_graph_db():
    pass

# We make one special job to materialize the entire graph
materialize_geoconnex_graph_db = define_asset_job(
    "materialize_geoconnex_graph_db",
    selection="geoconnex_graph_db",
)

all_jobs = individual_source_crawls + [materialize_geoconnex_graph_db]

# expose all the code needed for our dagster repo
definitions = Definitions(
    jobs=all_jobs,
    schedules=[ScheduleDefinition(
        name="materialize_geoconnex_schedule",
        description="Run all the jobs to materialize the geoconnex graph db",
        job=materialize_geoconnex_graph_db,
        cron_schedule="0 0 1 * *",
    )],
    assets=[geoconnex_graph_db],
    sensors=[
        dagster_slack.make_slack_on_run_failure_sensor(
            channel="#cgs-iow-bots",
            slack_token=strict_env("DAGSTER_SLACK_TOKEN"),
            text_fn=slack_error_fn,
            default_status=DefaultSensorStatus.RUNNING,
            monitor_all_code_locations=True,
            monitor_all_repositories=True,
            monitored_jobs=all_jobs
        )
    ],
    # Commented out but can uncomment if we want to send other slack msgs
    # resources={
    #     "slack": dagster_slack.SlackResource(token=strict_env("DAGSTER_SLACK_TOKEN")),
    # },
)
