from dataclasses import dataclass
import time
import os
import json
import io
from typing import TypeAlias

from docker.types import RestartPolicy, ServiceMode
from ec.gleanerio.gleaner import (
    getSitemapSourcesFromGleaner,
)
from minio import Minio
from minio.error import S3Error
from datetime import datetime
from ec.reporting.report import (
    missingReport,
    generateGraphReportsRepo,
    reportTypes,
    generateIdentifierRepo,
)
from ec.datastore import s3
from ec.summarize import summaryDF2ttl, get_summary4repoSubset
import requests
import logging as log

from typing import List, Literal, Optional, Sequence, Tuple

import docker
from dagster import op, graph, get_dagster_logger
from dagster import In, Nothing
from dagster._core.utils import parse_env_var

from dagster_docker.container_context import DockerContainerContext
from dagster_docker.docker_run_launcher import DockerRunLauncher
from dagster_docker.utils import validate_docker_image
from docker.types.services import ConfigReference

cli_modes: TypeAlias = Literal[
        "gleaner", 
        
        # All the cli modes that nabu can run
        "release",
        "object",
        "prune",
        "prov-release",
        "prov-clear",
        "prov-object",
        "prov-drain",
        "orgs-release",
        "orgs",
    ]

@dataclass
class S3ObjectInfo:
    """Metadata for uploading into S3"""
    bucket_name: str
    object_name: str

## Constants ##
DEBUG = os.getenv("DEBUG", "False").lower() == "true"
SUMMARY_PATH = "graphs/summary"
RELEASE_PATH = "graphs/latest"
GLEANER_HEADLESS_NETWORK = "headless_gleanerio"
GLEANERIO_LOG_PREFIX = "scheduler/logs/"
DAGSTER_GLEANER_CONFIG_PATH = "/opt/dagster/app/build/gleanerconfig.yaml"
if not os.path.exists(DAGSTER_GLEANER_CONFIG_PATH):
    raise Exception(
        f"Missing gleaner config file: Not located at {DAGSTER_GLEANER_CONFIG_PATH}"
    )
GLEANERIO_GLEANER_CONFIG_PATH = DAGSTER_GLEANER_CONFIG_PATH
if not os.path.exists(GLEANERIO_GLEANER_CONFIG_PATH):
    raise Exception(
        f"Missing gleaner config file: Not located at {GLEANERIO_GLEANER_CONFIG_PATH}"
    )
GLEANERIO_NABU_CONFIG_PATH = "/opt/dagster/app/build/nabuconfig.yaml"
if not os.path.exists(GLEANERIO_NABU_CONFIG_PATH):
    raise Exception(
        f"Missing nabu config file: Not located at {GLEANERIO_NABU_CONFIG_PATH}"
    )


## Env vars ##
def assert_all_vars():
    """If env vars aren't defined properly, we want to immediately catch
    this and fail early instead of spawning containers with empty env vars"""
    vars = [
        "GLEANERIO_MINIO_ADDRESS",
        "GLEANERIO_MINIO_PORT",
        "GLEANERIO_MINIO_USE_SSL",
        "GLEANERIO_MINIO_SECRET_KEY",
        "GLEANERIO_MINIO_ACCESS_KEY",
        "GLEANERIO_MINIO_BUCKET",
        "GLEANERIO_HEADLESS_ENDPOINT",
        "GLEANERIO_GRAPH_URL",
        "GLEANERIO_GRAPH_NAMESPACE",
    ]
    errors = ""
    for var in vars:
        if os.environ.get(var) is None:
            errors += f"Missing {var}, "
    if errors:
        raise Exception(errors)


assert_all_vars()


def strict_env(key: str):
    val = os.environ.get(key)
    if val is None:
        raise Exception(f"Missing {key}")

    return val


GLEANER_MINIO_ADDRESS = strict_env("GLEANERIO_MINIO_ADDRESS")
GLEANER_MINIO_PORT = strict_env("GLEANERIO_MINIO_PORT")
GLEANER_MINIO_USE_SSL = strict_env("GLEANERIO_MINIO_USE_SSL") in [
    True,
    "true",
    "True",
]
GLEANER_MINIO_SECRET_KEY = strict_env("GLEANERIO_MINIO_SECRET_KEY")
GLEANER_MINIO_ACCESS_KEY = strict_env("GLEANERIO_MINIO_ACCESS_KEY")
GLEANER_MINIO_BUCKET = strict_env("GLEANERIO_MINIO_BUCKET")
# set for the earhtcube utiltiies
MINIO_OPTIONS = {
    "secure": GLEANER_MINIO_USE_SSL,
    "access_key": GLEANER_MINIO_ACCESS_KEY,
    "secret_key": GLEANER_MINIO_SECRET_KEY,
}

GLEANER_HEADLESS_ENDPOINT = strict_env("GLEANERIO_HEADLESS_ENDPOINT")
# using GLEANER, even though this is a nabu property... same prefix seems easier
GLEANER_GRAPH_URL = strict_env("GLEANERIO_GRAPH_URL")
GLEANER_GRAPH_NAMESPACE = strict_env("GLEANERIO_GRAPH_NAMESPACE")
GLEANERIO_GLEANER_IMAGE = strict_env("GLEANERIO_GLEANER_IMAGE")
GLEANERIO_NABU_IMAGE = strict_env("GLEANERIO_NABU_IMAGE")
GLEANERIO_DATAGRAPH_ENDPOINT = strict_env("GLEANERIO_DATAGRAPH_ENDPOINT")
GLEANERIO_PROVGRAPH_ENDPOINT = strict_env("GLEANERIO_PROVGRAPH_ENDPOINT")


def _graphEndpoint():
    return f"{GLEANER_GRAPH_URL}/namespace/{GLEANER_GRAPH_NAMESPACE}/sparql"

def _graphSummaryEndpoint():
    return f"{GLEANER_GRAPH_URL}/namespace/{GLEANER_GRAPH_NAMESPACE}_summary/sparql"

def _pythonMinioAddress(url, port=None) -> str:
    """Construct a string for connecting to a minio S3 server"""
    if not url:
        raise RuntimeError("Tried to construct minio address with an empty URL")
    get_dagster_logger().info(f"Sending to data to s3 at: {url=}{port=}")
    return f"{url}:{port}" if port is not None else url

def s3reader(object_to_get):
    server = _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT)
    get_dagster_logger().info(f"S3 URL    : {GLEANER_MINIO_ADDRESS}")
    get_dagster_logger().info(f"S3 PYTHON SERVER : {server}")
    get_dagster_logger().info(f"S3 PORT   : {GLEANER_MINIO_PORT}")
    get_dagster_logger().info(f"S3 BUCKET : {GLEANER_MINIO_BUCKET}")
    get_dagster_logger().debug(f"S3 object : {str(object_to_get)}")

    client = Minio(
        server,
        secure=GLEANER_MINIO_USE_SSL,
        access_key=GLEANER_MINIO_ACCESS_KEY,
        secret_key=GLEANER_MINIO_SECRET_KEY,
    )
    try:
        return client.get_object(GLEANER_MINIO_BUCKET, object_to_get)
    except S3Error as err:
        get_dagster_logger().error(f"S3 read error : {str(err)}")
        raise err


def s3loader(data, name: str):
    """Load arbitrary data into the s3 bucket"""
    endpoint = _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT)

    client = Minio(
        endpoint,
        secure=GLEANER_MINIO_USE_SSL,
        access_key=GLEANER_MINIO_ACCESS_KEY,
        secret_key=GLEANER_MINIO_SECRET_KEY,
    )

    date_string = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

    logname = name + "_{}.log".format(date_string)
    objPrefix = GLEANERIO_LOG_PREFIX + logname
    f = io.BytesIO()
    length = f.write(data)
    f.seek(0)
    client.put_object(
        GLEANER_MINIO_BUCKET,
        objPrefix,
        f,  
        length,
        content_type="text/plain",
    )
    get_dagster_logger().info(f"Log uploaded: {str(objPrefix)}")


def post_to_graph(source, path=RELEASE_PATH, extension="nq", url=_graphEndpoint()):
    proto = "https" if GLEANER_MINIO_USE_SSL else "http"
    address = _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT)
    release_url = f"{proto}://{address}/{GLEANER_MINIO_BUCKET}/{path}/{source}_release.{extension}"

    get_dagster_logger().info(f'graph: insert "{source}" to {url} ')
    loadfrom = {"update": f"LOAD <{release_url}>"}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    r = requests.post(url, headers=headers, data=loadfrom)
    log.debug(f" status:{r.status_code}")  # status:404
    get_dagster_logger().info(f"graph: LOAD from {release_url}: status:{r.status_code}")
    if r.status_code == 200:
        get_dagster_logger().info(f"graph load response: {str(r.text)} ")
        # '<?xml version="1.0"?><data modified="0" milliseconds="7"/>'
        if "mutationCount=0" in r.text:
            get_dagster_logger().info("graph: no data inserted")
    else:
        get_dagster_logger().info(f"graph: error {str(r.text)}")
        raise Exception(
            f" graph: failed,  LOAD from {release_url}: status:{r.status_code}"
        )


def _create_service(
    client: docker.DockerClient,
    container_context: DockerContainerContext,
    image: str,
    command: Optional[Sequence[str]],
    name="",
    workingdir="/",
):
    """Given a client and image metadata, create a docker service and the associated container"""

    env_vars = dict([parse_env_var(env_var) for env_var in container_context.env_vars])
    get_dagster_logger().info(f"create docker service for {name}")
    get_dagster_logger().info(str(client.configs.list()))
    gleanerconfig = client.configs.list(filters={"name": ["gleaner"]})
    get_dagster_logger().info(f"docker config gleaner id {str(gleanerconfig[0].id)}")
    nabuconfig = client.configs.list(filters={"name": ["nabu"]})
    get_dagster_logger().info(f"docker config nabu id {str(nabuconfig[0].id)}")
    get_dagster_logger().info(f"create docker service for {name}")

    
    gleaner = ConfigReference(
        gleanerconfig[0].id,
        "gleaner",
        GLEANERIO_GLEANER_CONFIG_PATH,
    )
    nabu = ConfigReference(nabuconfig[0].id, "nabu", GLEANERIO_NABU_CONFIG_PATH)
    
    service = client.services.create(
        image,
        args=command,
        env=env_vars,
        name=name,
        networks=container_context.networks
        if len(container_context.networks)
        else None,
        restart_policy= RestartPolicy(condition="none"),
        mode=ServiceMode("replicated-job", concurrency=1, replicas=1),
        workdir=workingdir,
        configs=[gleaner, nabu],
    )

    ARBITRARY_SECONDS_TO_WAIT_UNTIL_FAILURE = 10
    for _ in range(0, ARBITRARY_SECONDS_TO_WAIT_UNTIL_FAILURE):
        get_dagster_logger().debug(str(service.tasks()))

        containers = client.containers.list(
            all=True, filters={"label": f"com.docker.swarm.service.name={name}"}
        )
        # Only spawn one container
        if len(containers) > 0:
            break
        time.sleep(1)
    else:
        raise RuntimeError(f"Container for service {name} not starting")

    get_dagster_logger().info(len(containers))
    return service, containers[0]


def get_cli_args(
    mode: cli_modes,
    source: str,
) -> Tuple[str, List[str], str, str]:
    """Given a mode and source return the cli args that are needed to run either gleaner or nabu"""
    if mode == "gleaner":
        IMAGE = GLEANERIO_GLEANER_IMAGE
        ARGS = ["--cfg", GLEANERIO_GLEANER_CONFIG_PATH, "-source", source, "--rude"]
        NAME = f"sch_{source}_{str(mode)}"
        WorkingDir = "/gleaner/"
    else:
        # Handle all nabu modes
        IMAGE = GLEANERIO_NABU_IMAGE
        WorkingDir = "/nabu/"
        NAME = f"sch_{source}_{str(mode)}"
        match mode:
            case "release":
                ARGS = [
                    "--cfg",
                    GLEANERIO_NABU_CONFIG_PATH,
                    "release",
                    "--prefix",
                    "summoned/" + source,
                ]
            case "object":
                ARGS = [
                    "--cfg",
                    GLEANERIO_NABU_CONFIG_PATH,
                    "object",
                    f"/graphs/latest/{source}_release.nq",
                    "--endpoint",
                    GLEANERIO_DATAGRAPH_ENDPOINT,
                ]
            case "prune":
                ARGS = [
                    "--cfg",
                    GLEANERIO_NABU_CONFIG_PATH,
                    "prune",
                    "--prefix",
                    "summoned/" + source,
                    "--endpoint",
                    GLEANERIO_DATAGRAPH_ENDPOINT,
                ]
            case "prov-release":
                ARGS = [
                    "--cfg",
                    GLEANERIO_NABU_CONFIG_PATH,
                    "release",
                    "--prefix",
                    "prov/" + source,
                ]

            case "prov-clear":
                ARGS = [
                    "--cfg",
                    GLEANERIO_NABU_CONFIG_PATH,
                    "clear",
                    "--endpoint",
                    "--endpoint",
                    GLEANERIO_PROVGRAPH_ENDPOINT,
                ]
            case "prov-object":
                ARGS = [
                    "--cfg",
                    GLEANERIO_NABU_CONFIG_PATH,
                    "object",
                    f"/graphs/latest/{source}_prov.nq",
                    "--endpoint",
                    GLEANERIO_PROVGRAPH_ENDPOINT,
                ]
            case "prov-drain":
                ARGS = [
                    "--cfg",
                    GLEANERIO_NABU_CONFIG_PATH,
                    "drain",
                    "--prefix",
                    "prov/" + source,
                ]
            case "orgs-release":
                ARGS = [
                    "--cfg",
                    GLEANERIO_NABU_CONFIG_PATH,
                    "release",
                    "--prefix",
                    "orgs",
                    "--endpoint",
                    GLEANERIO_DATAGRAPH_ENDPOINT,
                ]
            case "orgs":
                ARGS = [
                    "--cfg",
                    GLEANERIO_NABU_CONFIG_PATH,
                    "prefix",
                    "--prefix",
                    "orgs",
                    "--endpoint",
                    GLEANERIO_DATAGRAPH_ENDPOINT,
                ]
            case _:
                get_dagster_logger().error(f"Called gleaner with invalid mode: {mode}")
                return 1
    return IMAGE, ARGS, NAME, WorkingDir


def gleanerio(
    context,
    mode: cli_modes,
    source,
) -> int:

    get_dagster_logger().info(f"Gleanerio mode: {mode}")
    get_dagster_logger().info(f"Datagraph value: {GLEANERIO_DATAGRAPH_ENDPOINT}")
    get_dagster_logger().info(f"PROVgraph value: {GLEANERIO_PROVGRAPH_ENDPOINT}")

    IMAGE, ARGS, NAME, WorkingDir = get_cli_args(mode, source)

    run_container_context = DockerContainerContext.create_for_run(
        context.dagster_run,
        context.instance.run_launcher
        if isinstance(context.instance.run_launcher, DockerRunLauncher)
        else None,
    )
    validate_docker_image(IMAGE)

    try:
        get_dagster_logger().info("start docker code region: ")

        op_container_context = DockerContainerContext(
            networks=[GLEANER_HEADLESS_NETWORK],
            container_kwargs={
                "working_dir": WorkingDir,
            },
        )
        container_context = run_container_context.merge(op_container_context)
        get_dagster_logger().info("call docker _get_client: ")

        get_dagster_logger().info("try docker _create_service: ")
        service, container = _create_service(
            docker.DockerClient(version="1.43"),
            container_context,
            IMAGE,
            command=ARGS,
            name=NAME,
            workingdir=WorkingDir,
        )

        try:
            for line in container.logs(
                stdout=True, stderr=True, stream=True, follow=True
            ):
                get_dagster_logger().debug(line)  # noqa: T201s
        except docker.errors.APIError as ex:
            get_dagster_logger().info(
                f"This is ok. watch container logs failed Docker API ISSUE: {repr(ex)}"
            )

        # ## ------------  Wait expect 200
        # we want to get the logs, no matter what, so do not exit, yet.
        ## or should logs be moved into finally?
        ### in which case they need to be methods that don't send back errors.
        exit_status = container.wait()["StatusCode"]
        get_dagster_logger().info(f"Container Wait Exit status:  {exit_status}")
        # WE PULL THE LOGS, then will throw an error
        returnCode = exit_status

        logs = container.logs(stdout=True, stderr=True, stream=False, follow=False).decode(
            "latin-1"
        )

        s3loader(str(logs).encode(), NAME)

        get_dagster_logger().info("container Logs to s3: ")

        if exit_status != 0:
            raise Exception(f"Gleaner/Nabu container returned exit code {exit_status}")
    finally:
        if not DEBUG:
            try:
                if service:
                    service.remove()
                    get_dagster_logger().info(f"Service Remove: {service.name}")
            except:
                get_dagster_logger().info(f"Service Not created, so not removed.")

        else:
            get_dagster_logger().info(
                f"Service {service.name} NOT Removed : DEBUG ENABLED"
            )

    if returnCode != 0:
        get_dagster_logger().error(
            f"Gleaner/Nabu container {returnCode} exit code. See logs in S3"
        )
        raise Exception("Gleaner/Nabu container non-zero exit code. See logs in S3")
    return 0


# #########################  Ops Section
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



# #########################  Directed Graph Section
@graph
def harvest_SOURCEVAL():
    # check for containers
    containers = SOURCEVAL_getImage()

    # conduct the harvest
    harvest = SOURCEVAL_gleaner(start=containers)

    # data branch
    load_release = SOURCEVAL_naburelease(start=harvest)
    load_uploadrelease = SOURCEVAL_uploadrelease(start=load_release)
    load_prune = SOURCEVAL_nabu_prune(start=load_uploadrelease)

    # prov branch
    prov_release = SOURCEVAL_nabu_provrelease(start=harvest)
    prov_clear = SOURCEVAL_nabu_provclear(start=prov_release)
    prov_object = SOURCEVAL_nabu_provobject(start=prov_clear)
    prov_drain = SOURCEVAL_nabu_provdrain(start=prov_object)

    # org branch
    org_release = SOURCEVAL_nabu_orgsrelease(start=harvest)
    load_org = SOURCEVAL_nabuorgs(start=org_release)

    # reports branch
    # report_ms3 = SOURCEVAL_missingreport_s3(start=harvest)
    # report_idstat = SOURCEVAL_identifier_stats(start=report_ms3)
    # report_bucketurl = SOURCEVAL_bucket_urls( start=report_idstat)
    # for some reason, this causes a msg parameter missing

    # summarize
    # summarize = SOURCEVAL_summarize(start=load_uploadrelease)
    # upload_summarize = SOURCEVAL_upload_summarize(start=summarize)

    # run after load
    # report_msgraph = SOURCEVAL_missingreport_graph(start=summarize)
    # report_graph = SOURCEVAL_graph_reports(start=report_msgraph)
