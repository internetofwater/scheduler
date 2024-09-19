from dagster import (
    DefaultScheduleStatus,
    Definitions,
    In,
    JobDefinition,
    Nothing,
    OpExecutionContext,
    ScheduleDefinition,
    get_dagster_logger,
    op,
    graph,
)
import docker

from lib.utils import (
    _graphEndpoint,
    _graphSummaryEndpoint,
    _pythonMinioAddress,
    run_gleaner,
    post_to_graph,
)
import yaml

from lib.env import (
    GLEANER_MINIO_ADDRESS,
    GLEANER_MINIO_BUCKET,
    GLEANER_MINIO_PORT,
    GLEANERIO_GLEANER_IMAGE,
    GLEANERIO_NABU_IMAGE,
    MINIO_OPTIONS,
    SUMMARY_PATH,
    GLEANER_CONFIG_PATH,
)
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

from lib.types import GleanerSource, S3ObjectInfo


def strict_get_tag(context: OpExecutionContext, key: str) -> str:
    src = context.run_tags[key]
    if src is None:
        raise Exception(f"Missing run tag {key}")
    return src


@op
def pull_docker_images():
    get_dagster_logger().info("Getting docker client and pulling images: ")
    client = docker.DockerClient(version="1.43")
    client.images.pull(GLEANERIO_GLEANER_IMAGE)
    client.images.pull(GLEANERIO_NABU_IMAGE)


@op(ins={"start": In(Nothing)})
def gleaner(context: OpExecutionContext):
    source = strict_get_tag(context, "source")

    returned_value = run_gleaner(context, "gleaner", source)
    get_dagster_logger().info(f"Gleaner returned value: '{returned_value}'")


@op(ins={"start": In(Nothing)})
def naburelease(context: OpExecutionContext):
    source = strict_get_tag(context, "source")

    returned_value = run_gleaner(context, "release", source)
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu release returned  {r} ")


@op(ins={"start": In(Nothing)})
def uploadrelease(context: OpExecutionContext):
    source = strict_get_tag(context, "source")
    returned_value = run_gleaner(context, "object", source)
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu object call release returned  {r} ")


@op(ins={"start": In(Nothing)})
def nabu_prune(context: OpExecutionContext):
    source = strict_get_tag(context, "source")
    returned_value = run_gleaner(context, "prune", source)
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu prune returned  {r} ")


@op(ins={"start": In(Nothing)})
def nabu_provrelease(context):
    source = strict_get_tag(context, "source")
    returned_value = run_gleaner(context, "prov-release", source)
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu prov-release returned  {r} ")


@op(ins={"start": In(Nothing)})
def nabu_provclear(context: OpExecutionContext):
    source = strict_get_tag(context, "source")
    returned_value = run_gleaner(context, "prov-clear", source)
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu prov-clear returned  {r} ")


@op(ins={"start": In(Nothing)})
def nabu_provobject(context):
    source = strict_get_tag(context, "source")
    returned_value = run_gleaner(context, "prov-object", source)
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu prov-object returned  {r} ")


@op(ins={"start": In(Nothing)})
def nabu_provdrain(context: OpExecutionContext):
    source = strict_get_tag(context, "source")
    returned_value = run_gleaner(context, "prov-drain", source)
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu prov-drain returned  {r} ")


@op(ins={"start": In(Nothing)})
def nabu_orgsrelease(context: OpExecutionContext):
    source = strict_get_tag(context, "source")
    returned_value = run_gleaner(context, "orgs-release", source)
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu orgs-release returned  {r} ")


@op(ins={"start": In(Nothing)})
def nabuorgs(context: OpExecutionContext):
    source = strict_get_tag(context, "source")
    returned_value = run_gleaner(context, ("orgs"), source)
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"nabu org load returned  {r} ")


@op(ins={"start": In(Nothing)})
def missingreport_s3(context: OpExecutionContext):
    source = strict_get_tag(context, "source")
    source = getSitemapSourcesFromGleaner(GLEANER_CONFIG_PATH, sourcename=source)
    source_url = source.get("url")
    s3Minio = s3.MinioDatastore(
        _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT), MINIO_OPTIONS
    )
    bucket = GLEANER_MINIO_BUCKET
    source_name = source
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
def missingreport_graph(context: OpExecutionContext):
    source = strict_get_tag(context, "source")
    source = getSitemapSourcesFromGleaner(GLEANER_CONFIG_PATH, sourcename=source)
    source_url = source.get("url")
    s3Minio = s3.MinioDatastore(
        _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT), MINIO_OPTIONS
    )
    bucket = GLEANER_MINIO_BUCKET
    source_name = source

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
def graph_reports(context: OpExecutionContext):
    source = strict_get_tag(context, "source")

    source = getSitemapSourcesFromGleaner(GLEANER_CONFIG_PATH, sourcename=source)
    s3Minio = s3.MinioDatastore(
        _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT), MINIO_OPTIONS
    )
    bucket = GLEANER_MINIO_BUCKET
    source_name = source

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
def identifier_stats(context: OpExecutionContext):
    source = strict_get_tag(context, "source")

    source = getSitemapSourcesFromGleaner(GLEANER_CONFIG_PATH, sourcename=source)
    s3Minio = s3.MinioDatastore(
        _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT), MINIO_OPTIONS
    )
    bucket = GLEANER_MINIO_BUCKET
    source_name = source

    returned_value = generateIdentifierRepo(source_name, bucket, s3Minio)
    r = str("returned value:{}".format(returned_value))
    # r = str('identifier stats returned value:{}'.format(returned_value))
    report = returned_value.to_json()
    s3Minio.putReportFile(bucket, source_name, "identifier_stats.json", report)
    get_dagster_logger().info(f"identifer stats report  returned  {r} ")


@op(ins={"start": In(Nothing)})
def bucket_urls(context: OpExecutionContext):
    source = strict_get_tag(context, "source")

    s3Minio = s3.MinioDatastore(
        f"{GLEANER_MINIO_ADDRESS}:{GLEANER_MINIO_PORT}", MINIO_OPTIONS
    )
    bucket = GLEANER_MINIO_BUCKET
    source_name = source

    res = s3Minio.listSummonedUrls(bucket, source_name)
    bucketurls = json.dumps(res, indent=2)
    s3Minio.putReportFile(
        GLEANER_MINIO_BUCKET, source_name, "bucketutil_urls.json", bucketurls
    )
    get_dagster_logger().info(f"bucker urls report  returned value: {res} ")


@op(ins={"start": In(Nothing)})
def summarize(context: OpExecutionContext):
    source = strict_get_tag(context, "source")

    s3Minio = s3.MinioDatastore(
        _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT), MINIO_OPTIONS
    )
    bucket = GLEANER_MINIO_BUCKET
    source_name = source
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
def upload_summarize(context: OpExecutionContext):
    source = strict_get_tag(context, "source")
    returned_value = post_to_graph(
        source,
        path=SUMMARY_PATH,
        extension="ttl",
        graphendpoint=_graphSummaryEndpoint(),
    )
    # the above can be done (with a better path approach) in Nabu
    # returned_value = gleanerio(context, ("object"), source)
    r = str("returned value:{}".format(returned_value))
    get_dagster_logger().info(f"upload summary returned  {r} ")


@graph
def harvest():
    """
    Harvest all assets for a given source.
    All source specific info is passed via tags within the run context
    """

    get_dagster_logger().info("Harvesting source")

    # dagster links between ops with arguments as dependencies.
    setup = gleaner(start=pull_docker_images())

    # # # data branch
    release = naburelease(start=setup)
    upload = uploadrelease(start=release)
    nabu_prune(start=upload)

    # # prov branch
    provrelease = nabu_provrelease(start=setup)
    clear = nabu_provclear(start=provrelease)
    obj = nabu_provobject(start=clear)
    nabu_provdrain(start=obj)

    # # org branch
    org_release = nabu_orgsrelease(start=setup)
    nabuorgs(start=org_release)


def generate_job_and_schedules(
    source: GleanerSource,
) -> tuple[JobDefinition, ScheduleDefinition]:
    job = harvest.to_job(
        name="harvest_" + source["name"],
        description=f"harvest all assets for {source['name']}",
        tags={"source": source["name"]},
    )

    # Define the schedule for the job
    schedule = ScheduleDefinition(
        job=job,
        # “At 23:59 on day-of-month 1.”
        cron_schedule="59 23 1 * *",
        default_status=DefaultScheduleStatus.STOPPED,
    )

    return (job, schedule)


def get_gleaner_config_sources() -> list[GleanerSource]:
    """Given a config, return the jobs that will need to be run to perform a full geoconnex crawl"""
    with open(GLEANER_CONFIG_PATH) as f:
        config = yaml.safe_load(f)
        all_sources = [site for site in config["sources"]]
        assert len(all_sources) > 0
        return all_sources


sources = get_gleaner_config_sources()
jobs, schedules = [], []
for src in sources:
    jbs, schd = generate_job_and_schedules(src)
    jobs += [jbs]
    schedules += [schd]

# definitions instantiate all the jobs and schedules for a dagster repo
definitions = Definitions(
    jobs=jobs,
    schedules=schedules,
)
