from ast import Tuple
from calendar import Day
from dagster import DefaultScheduleStatus, Definitions, DynamicOut, DynamicOutput, In, JobDefinition, Nothing, ScheduleDefinition, get_dagster_logger, job, op, graph, schedule, asset
import docker

from .lib import gleanerio
import yaml

from .env import GLEANERIO_GLEANER_IMAGE, GLEANERIO_NABU_IMAGE, SUMMARY_PATH, GLEANER_CONFIG_PATH
import json

from ec.gleanerio.gleaner import (
    getSitemapSourcesFromGleaner,
)
from ec.reporting.report import (
    missingReport,
    generateGraphReportsRepo,
    reportTypes,
    generateIdentifierRepo,
)
from ec.datastore import s3
from ec.summarize import summaryDF2ttl, get_summary4repoSubset

import docker
from dagster import op, graph, get_dagster_logger
from dagster import In, Nothing
from dagster._core.utils import parse_env_var

from dagster_docker.container_context import DockerContainerContext
from dagster_docker.docker_run_launcher import DockerRunLauncher
from dagster_docker.utils import validate_docker_image
from docker.types.services import ConfigReference
from .types import GleanerSource

@op(out=DynamicOut())
def get_gleaner_config_sources() -> list[DynamicOutput[GleanerSource]]:
    """Given a config, return the jobs that will need to be run to perform a crawl"""
    # mimicking getSitemapSourcesFromGleaner from earthcube-utils
    with open(GLEANER_CONFIG_PATH) as f:
        config = yaml.safe_load(f)
        all_sources = [site for site in config["sources"]]
        assert len(all_sources) > 0
        return all_sources



@job
def construct_repository():
    # the repository.py file is 

    def generate_job_and_schedules(source: GleanerSource):

        job = JobDefinition(
            
        )

        schedule = ScheduleDefinition(
            job=job,
            cron_schedule="15 5 * * 1-5",
            default_status=DefaultScheduleStatus.STOPPED,
        )



    get_gleaner_config_sources()

    

    definitions = Definitions(
        # https://docs.dagster.io/concepts/automation/schedules/automating-ops-schedules-jobs
        jobs=[],
        schedules=[],
    )


@op
def SOURCEVAL_getImage(context):
    get_dagster_logger().info("Getting docker client and pulling images: ")
    client = docker.DockerClient(version="1.43")
    client.images.pull(GLEANERIO_GLEANER_IMAGE)
    client.images.pull(GLEANERIO_NABU_IMAGE)


@op(ins={"start": In(Nothing)})
def SOURCEVAL_gleaner(context):
    returned_value = gleanerio(context, "gleaner", "SOURCEVAL")
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"Gleaner returned  {r} ")
    return


@op(ins={"start": In(Nothing)})
def SOURCEVAL_naburelease(context):
    returned_value = gleanerio(context, "release", "SOURCEVAL")
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu release returned  {r} ")

@op(ins={"start": In(Nothing)})
def SOURCEVAL_uploadrelease(context):
    returned_value = gleanerio(context, "object", "SOURCEVAL")
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu object call release returned  {r} ")


@op(ins={"start": In(Nothing)})
def SOURCEVAL_nabu_prune(context):
    returned_value = gleanerio(context, "prune", "SOURCEVAL")
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu prune returned  {r} ")

@op(ins={"start": In(Nothing)})
def SOURCEVAL_nabu_provrelease(context):
    returned_value = gleanerio(context, "prov-release", "SOURCEVAL")
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu prov-release returned  {r} ")

@op(ins={"start": In(Nothing)})
def SOURCEVAL_nabu_provclear(context):
    returned_value = gleanerio(context, "prov-clear", "SOURCEVAL")
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu prov-clear returned  {r} ")

@op(ins={"start": In(Nothing)})
def SOURCEVAL_nabu_provobject(context):
    returned_value = gleanerio(context, "prov-object", "SOURCEVAL")
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu prov-object returned  {r} ")

@op(ins={"start": In(Nothing)})
def SOURCEVAL_nabu_provdrain(context):
    returned_value = gleanerio(context, "prov-drain", "SOURCEVAL")
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu prov-drain returned  {r} ")

@op(ins={"start": In(Nothing)})
def SOURCEVAL_nabu_orgsrelease(context):
    returned_value = gleanerio(context, "orgs-release", "SOURCEVAL")
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu orgs-release returned  {r} ")


@op(ins={"start": In(Nothing)})
def SOURCEVAL_nabuorgs(context):
    returned_value = gleanerio(context, ("orgs"), "SOURCEVAL")
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu org load returned  {r} ")

@op(ins={"start": In(Nothing)})
def SOURCEVAL_missingreport_s3(context):
    source = getSitemapSourcesFromGleaner(
        DAGSTER_GLEANER_CONFIG_PATH, sourcename="SOURCEVAL"
    )
    source_url = source.get("url")
    s3Minio = s3.MinioDatastore(
        _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT), MINIO_OPTIONS
    )
    bucket = GLEANER_MINIO_BUCKET
    source_name = "SOURCEVAL"
    graphendpoint = None
    milled = False
    summon = True
    returned_value = missingReport(
        source_url,
        bucket,
        source_name,
        s3Minio,
        graphendpoint,
        milled=milled,
        summon=summon,
    )
    r = str("missing repoort returned value:{}".format(returned_value))
    report = json.dumps(returned_value, indent=2)
    s3Minio.putReportFile(bucket, source_name, "missing_report_s3.json", report)
    get_dagster_logger().info(f"missing s3 report  returned  {r} ")


@op(ins={"start": In(Nothing)})
def SOURCEVAL_missingreport_graph(context):
    source = getSitemapSourcesFromGleaner(
        DAGSTER_GLEANER_CONFIG_PATH, sourcename="SOURCEVAL"
    )
    source_url = source.get("url")
    s3Minio = s3.MinioDatastore(
        _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT), MINIO_OPTIONS
    )
    bucket = GLEANER_MINIO_BUCKET
    source_name = "SOURCEVAL"

    graphendpoint = _graphEndpoint() 

    milled = True
    summon = False  # summon only off
    returned_value = missingReport(
        source_url,
        bucket,
        source_name,
        s3Minio,
        graphendpoint,
        milled=milled,
        summon=summon,
    )
    r = str("missing report graph returned value:{}".format(returned_value))
    report = json.dumps(returned_value, indent=2)

    s3Minio.putReportFile(bucket, source_name, "missing_report_graph.json", report)
    get_dagster_logger().info(f"missing graph  report  returned  {r} ")

@op(ins={"start": In(Nothing)})
def SOURCEVAL_graph_reports(context):
    source = getSitemapSourcesFromGleaner(
        DAGSTER_GLEANER_CONFIG_PATH, sourcename="SOURCEVAL"
    )
    s3Minio = s3.MinioDatastore(
        _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT), MINIO_OPTIONS
    )
    bucket = GLEANER_MINIO_BUCKET
    source_name = "SOURCEVAL"

    graphendpoint = _graphEndpoint() 

    returned_value = generateGraphReportsRepo(
        source_name, graphendpoint, reportList=reportTypes["repo_detailed"]
    )
    r = str("returned value:{}".format(returned_value))
    # report = json.dumps(returned_value, indent=2) # value already json.dumps
    report = returned_value
    s3Minio.putReportFile(bucket, source_name, "graph_stats.json", report)
    get_dagster_logger().info(f"graph report  returned  {r} ")


@op(ins={"start": In(Nothing)})
def SOURCEVAL_identifier_stats(context):
    source = getSitemapSourcesFromGleaner(
        DAGSTER_GLEANER_CONFIG_PATH, sourcename="SOURCEVAL"
    )
    s3Minio = s3.MinioDatastore(
        _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT), MINIO_OPTIONS
    )
    bucket = GLEANER_MINIO_BUCKET
    source_name = "SOURCEVAL"

    returned_value = generateIdentifierRepo(source_name, bucket, s3Minio)
    r = str("returned value:{}".format(returned_value))
    # r = str('identifier stats returned value:{}'.format(returned_value))
    report = returned_value.to_json()
    s3Minio.putReportFile(bucket, source_name, "identifier_stats.json", report)
    get_dagster_logger().info(f"identifer stats report  returned  {r} ")


@op(ins={"start": In(Nothing)})
def SOURCEVAL_bucket_urls(context):
    s3Minio = s3.MinioDatastore(
        _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT), MINIO_OPTIONS
    )
    bucket = GLEANER_MINIO_BUCKET
    source_name = "SOURCEVAL"

    res = s3Minio.listSummonedUrls(bucket, source_name)
    r = str("returned value:{}".format(res))
    bucketurls = json.dumps(res, indent=2)
    s3Minio.putReportFile(
        GLEANER_MINIO_BUCKET, source_name, "bucketutil_urls.json", bucketurls
    )
    get_dagster_logger().info(f"bucker urls report  returned  {r} ")

@op(ins={"start": In(Nothing)})
def SOURCEVAL_summarize(context):
    s3Minio = s3.MinioDatastore(
        _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT), MINIO_OPTIONS
    )
    bucket = GLEANER_MINIO_BUCKET
    source_name = "SOURCEVAL"
    endpoint = _graphEndpoint()  # getting data, not uploading data
    summary_namespace = _graphSummaryEndpoint()

    try:
        summarydf = get_summary4repoSubset(endpoint, source_name)
        nt, g = summaryDF2ttl(summarydf, source_name)  # let's try the new generator
        summaryttl = g.serialize(format="longturtle")
        # Lets always write out file to s3, and insert as a separate process
        # we might be able to make this an asset..., but would need to be acessible by http
        # if not stored in s3
        objectname = f"{SUMMARY_PATH}/{source_name}_release.ttl"  # needs to match that is expected by post

        s3Minio.putTextFileToStore(summaryttl, S3ObjectInfo(bucket, objectname))
        # inserted = sumnsgraph.insert(bytes(summaryttl, 'utf-8'), content_type="application/x-turtle")
        # if not inserted:
        #    raise Exception("Loading to graph failed.")
    except Exception as e:
        # use dagster logger
        get_dagster_logger().error(f"Summary. Issue creating graph  {str(e)} ")
        raise Exception(f"Loading Summary graph failed. {str(e)}")


@op(ins={"start": In(Nothing)})
def SOURCEVAL_upload_summarize(context):
    returned_value = post_to_graph(
        "SOURCEVAL",
        path=SUMMARY_PATH,
        extension="ttl",
        graphendpoint=_graphSummaryEndpoint(),
    )
    # the above can be done (with a better path approach) in Nabu
    # returned_value = gleanerio(context, ("object"), "SOURCEVAL")
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"upload summary returned  {r} ")


@graph
def harvest(source):
    # check for containers
    containers = getImage()

    # conduct the harvest
    harvest = gleaner(start=containers)

    # data branch
    load_release = naburelease(start=harvest)
    load_uploadrelease = uploadrelease(start=load_release)
    load_prune = nabu_prune(start=load_uploadrelease)

    # prov branch
    prov_release = nabu_provrelease(start=harvest)
    prov_clear = nabu_provclear(start=prov_release)
    prov_object = nabu_provobject(start=prov_clear)
    prov_drain = nabu_provdrain(start=prov_object)

    # org branch
    org_release = nabu_orgsrelease(start=harvest)
    load_org = nabuorgs(start=org_release)