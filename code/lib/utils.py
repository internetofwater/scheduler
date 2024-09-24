from datetime import datetime
import io
import time
from typing import Any, List, Optional, Sequence
from dagster import OpExecutionContext, RunFailureSensorContext, get_dagster_logger
from dagster_docker import DockerRunLauncher
import docker
import docker.errors
from minio import Minio, S3Error
import requests
from .env import (
    GLEANER_GRAPH_NAMESPACE,
    GLEANER_GRAPH_URL,
    GLEANER_HEADLESS_NETWORK,
    GLEANER_MINIO_ACCESS_KEY,
    GLEANER_MINIO_ADDRESS,
    GLEANER_MINIO_BUCKET,
    GLEANER_MINIO_PORT,
    GLEANER_MINIO_SECRET_KEY,
    GLEANER_MINIO_USE_SSL,
    GLEANERIO_DATAGRAPH_ENDPOINT,
    GLEANERIO_GLEANER_CONFIG_PATH,
    GLEANERIO_NABU_CONFIG_PATH,
    GLEANERIO_PROVGRAPH_ENDPOINT,
    RELEASE_PATH,
)
from docker.types.services import ConfigReference
from dagster_docker.container_context import DockerContainerContext
from docker.types import RestartPolicy, ServiceMode
from dagster_docker.utils import validate_docker_image
from dagster._core.utils import parse_env_var


def s3loader(
    data: Any,
    # path relative from the root bucket. i.e. 'foo/bar/baz.txt' -> gleanerbucket/foo/bar/baz.txt
    path: str,
):
    """Load arbitrary data into the s3 bucket"""
    endpoint = _pythonMinioAddress(GLEANER_MINIO_ADDRESS, GLEANER_MINIO_PORT)

    client = Minio(
        endpoint,
        secure=GLEANER_MINIO_USE_SSL,
        access_key=GLEANER_MINIO_ACCESS_KEY,
        secret_key=GLEANER_MINIO_SECRET_KEY,
    )

    f = io.BytesIO()
    length = f.write(data)
    f.seek(0)
    client.put_object(
        GLEANER_MINIO_BUCKET,
        path,
        f,
        length,
        content_type="text/plain",
    )
    get_dagster_logger().info(f"Uploaded '{path.split('/')[-1]}'")


def s3_log_loader(data: Any, name: str):
    date_string = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    filename = name + f"_{date_string}.log"
    s3loader(data, path=f"scheduler/logs/{filename}")


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
    gleanerconfig = client.configs.list(filters={"name": ["gleaner"]})
    nabuconfig = client.configs.list(filters={"name": ["nabu"]})
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
        restart_policy=RestartPolicy(condition="none"),
        mode=ServiceMode("replicated-job", concurrency=1, replicas=1),
        workdir=workingdir,
        configs=[gleaner, nabu],
    )

    # If the docker container is not created within 10 seconds, raise an error
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

    get_dagster_logger().info(
        f"Spawned {len(containers)} containers for service {name}"
    )
    return service, containers[0]


def run_scheduler_docker_image(
    context: OpExecutionContext,
    source: str,  # which organization we are crawling
    # the name of the docker image to pull and validate
    image_name: str,
    # the list of arguments to pass to the gleaner/nabu command
    args: List[str],
    # the name of the action to run inside gleaner/nabu
    action_name: str,
) -> int:
    """Run a docker image inside the context of dagster"""
    container_name = f"sch_{source}_{action_name}"
    WorkingDir = "/nabu/" if "nabu" in image_name else "/gleaner/"

    get_dagster_logger().info(f"Datagraph value: {GLEANERIO_DATAGRAPH_ENDPOINT}")
    get_dagster_logger().info(f"PROVgraph value: {GLEANERIO_PROVGRAPH_ENDPOINT}")

    run_container_context = DockerContainerContext.create_for_run(
        context.dagster_run,
        context.instance.run_launcher
        if isinstance(context.instance.run_launcher, DockerRunLauncher)
        else None,
    )
    validate_docker_image(image_name)

    # Create a service var at the beginning of the function so we can check against
    # it during cleanup to see if the service was created.
    service = None
    try:
        op_container_context = DockerContainerContext(
            networks=[GLEANER_HEADLESS_NETWORK],
            container_kwargs={
                "working_dir": WorkingDir,
            },
        )
        container_context = run_container_context.merge(op_container_context)

        get_dagster_logger().info(
            f"Spinning up {container_name=} with {image_name=}, and {args=}"
        )
        service, container = _create_service(
            docker.DockerClient(version="1.43"),
            container_context,
            image=image_name,
            command=args,
            name=container_name,
            workingdir=WorkingDir,
        )

        try:
            for line in container.logs(
                stdout=True, stderr=True, stream=True, follow=True
            ):
                # NOTE: we can potentially raise an error coming from inside the container here
                # if it is output via the log (i.e. timeouts)
                get_dagster_logger().debug(line) 
        except docker.errors.APIError as ex:
            get_dagster_logger().warning(
                f"Caught potential docker API issue for {container_name}: {ex}"
            )

        exit_status: int = container.wait()["StatusCode"]
        get_dagster_logger().info(f"Container Wait Exit status:  {exit_status}")

        logs = container.logs(
            stdout=True, stderr=True, stream=False, follow=False
        ).decode("latin-1")
        # Send the logs, then check the exit status to throw the error if needed
        s3_log_loader(str(logs).encode(), container_name)

        get_dagster_logger().info("Sent container Logs to s3: ")

        if exit_status != 0:
            get_dagster_logger().error(
                f"Gleaner/Nabu container returned exit code {exit_status}. See logs in S3"
            )
            raise Exception(
                f"Gleaner/Nabu container returned exit code {exit_status}. See logs in S3 "
            )
    finally:
        if service:
            service.remove()
            get_dagster_logger().info(f"Service Remove: {service.name}")

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
        get_dagster_logger().error(f"S3 read error : {err}")
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


def slack_error_fn(context: RunFailureSensorContext) -> str:
    get_dagster_logger().info("Sending notification to slack")
    # The make_slack_on_run_failure_sensor automatically sends the job
    # id and name so you can just send the error. We don't need other data in the string
    return f"Error: {context.failure_event.message}"
