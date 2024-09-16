import time
from typing import List, Optional, Sequence, Tuple

from dagster import get_dagster_logger
from dagster_docker import DockerRunLauncher
import docker
from minio import Minio, S3Error
import requests
from .env import GLEANER_GRAPH_NAMESPACE, GLEANER_GRAPH_URL, GLEANER_HEADLESS_NETWORK, GLEANER_MINIO_ACCESS_KEY, GLEANER_MINIO_ADDRESS, GLEANER_MINIO_BUCKET, GLEANER_MINIO_PORT, GLEANER_MINIO_SECRET_KEY, GLEANER_MINIO_USE_SSL, GLEANERIO_DATAGRAPH_ENDPOINT, GLEANERIO_GLEANER_IMAGE, GLEANERIO_GLEANER_CONFIG_PATH, GLEANERIO_NABU_IMAGE, GLEANERIO_NABU_CONFIG_PATH, GLEANERIO_PROVGRAPH_ENDPOINT, RELEASE_PATH
from .types import cli_modes
from docker.types.services import ConfigReference
from dagster_docker.container_context import DockerContainerContext
from docker.types import RestartPolicy, ServiceMode
from dagster_docker.utils import validate_docker_image
from dagster._core.utils import parse_env_var


def get_cli_args(
    action_to_run: cli_modes,
    source: str,
) -> Tuple[str, List[str], str, str]:
    """Given a mode and source return the cli args that are needed to run either gleaner or nabu"""
    if action_to_run == "gleaner":
        IMAGE = GLEANERIO_GLEANER_IMAGE
        ARGS = ["--cfg", GLEANERIO_GLEANER_CONFIG_PATH, "-source", source, "--rude"]
        NAME = f"sch_{source}_{action_to_run}"
        WorkingDir = "/gleaner/"
    else:
        # Handle all nabu modes
        IMAGE = GLEANERIO_NABU_IMAGE
        WorkingDir = "/nabu/"
        NAME = f"sch_{source}_{str(action_to_run)}"
        match action_to_run:
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
                get_dagster_logger().error(f"Called gleaner with invalid mode: {action_to_run}")
                raise ValueError(f"Called gleaner with invalid mode: {action_to_run}")
    return IMAGE, ARGS, NAME, WorkingDir

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
        # Only spawn one container; once it is spawned we are done
        if len(containers) > 0:
            break
        time.sleep(1)
    else:
        raise RuntimeError(f"Container for service {name} not starting")

    get_dagster_logger().info(len(containers))
    return service, containers[0]


def run_gleaner(
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
    
def post_to_graph(source, path=RELEASE_PATH, extension="nq", url=_graphEndpoint()):
    proto = "https" if GLEANER_MINIO_USE_SSL else "http"
    address = _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT)
    release_url = f"{proto}://{address}/{GLEANER_MINIO_BUCKET}/{path}/{source}_release.{extension}"

    get_dagster_logger().info(f'graph: insert "{source}" to {url} ')
    loadfrom = {"update": f"LOAD <{release_url}>"}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    r = requests.post(url, headers=headers, data=loadfrom)
    get_dagster_logger().info(f"graph: LOAD from {release_url}: status:{r.status_code}")
    if r.status_code == 200:
        get_dagster_logger().info(f"graph load response: {str(r.text)} ")
        # '<?xml version="1.0"?><data modified="0" milliseconds="7"/>'
        if "mutationCount=0" in r.text:
            get_dagster_logger().info("graph: no data inserted")
    else:
        get_dagster_logger().info(f"graph: error {str(r.text)}")
        raise Exception(
            f" graph: failed, LOAD from {release_url}: status:{r.status_code}"
        )

