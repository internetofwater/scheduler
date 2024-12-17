from datetime import datetime
import os
import re
import time
from typing import List, Optional, Sequence
from dagster import (
    AssetKey,
    OpExecutionContext,
    RunFailureSensorContext,
    get_dagster_logger,
)
from dagster_docker import DockerRunLauncher
import docker
import docker.errors
import docker.models
import docker.models.containers
import docker.models.services
from jinja2 import Environment, FileSystemLoader
import jinja2
from .dagster_env import sources_partitions_def

from .classes import S3
from .env import (
    GLEANERIO_DATAGRAPH_ENDPOINT,
    GLEANERIO_PROVGRAPH_ENDPOINT,
    strict_env,
)
from docker.types.services import ConfigReference
from dagster_docker.container_context import DockerContainerContext
from docker.types import RestartPolicy, ServiceMode
from dagster_docker.utils import validate_docker_image
from dagster._core.utils import parse_env_var


def remove_non_alphanumeric(string):
    return re.sub(r"[^a-zA-Z0-9_]+", "", string)


def create_service(
    client: docker.DockerClient,
    container_context: DockerContainerContext,
    image: str,
    command: Optional[Sequence[str]],
    name="",
) -> tuple[docker.models.services.Service, docker.models.containers.Container]:
    """Given a client and image metadata, create a docker service and the associated container"""

    env_vars = dict([parse_env_var(env_var) for env_var in container_context.env_vars])

    gleanerconfig = client.configs.list(filters={"name": ["gleaner"]})
    nabuconfig = client.configs.list(filters={"name": ["nabu"]})
    get_dagster_logger().info(f"creating docker service for {name}")

    gleaner = ConfigReference(
        config_id=gleanerconfig[0].id,
        config_name="gleaner",
        filename="gleanerconfig.yaml",
    )
    nabu = ConfigReference(
        config_id=nabuconfig[0].id, config_name="nabu", filename="nabuconfig.yaml"
    )

    service: docker.models.services.Service = client.services.create(
        image,
        args=command,
        env=env_vars,
        name=name,
        networks=(
            container_context.networks if len(container_context.networks) else None
        ),
        restart_policy=RestartPolicy(condition="none"),
        # Replicated jobs terminate after run
        # TODO: There is still a potential error here. If a container fails,
        # the job finishes but still appears in the swarm stack
        # This might cause an issue
        mode=ServiceMode("replicated-job", concurrency=1, replicas=1),
        configs=[gleaner, nabu],
    )

    ARBITRARY_SECONDS_TO_WAIT_UNTIL_FAILURE = 10
    for _ in range(0, ARBITRARY_SECONDS_TO_WAIT_UNTIL_FAILURE):
        # We need to get the handle to the container to stream the logs,
        # but if the container doesn't exist yet, it won't be in the list so we wait
        get_dagster_logger().debug(str(service.tasks()))

        containers: list[docker.models.containers.Container] = client.containers.list(
            # unclear why all=True is needed here
            all=True,
            filters={"label": f"com.docker.swarm.service.name={name}"},
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
    image_name: str,  # the name of the docker image to pull and validate
    args: List[str],  # the list of arguments to pass to the gleaner/nabu command
    action_name: str,  # the name of the action to run inside gleaner/nabu
):
    """Run a docker image inside the dagster docker runtime"""
    container_name = f"{source}_{action_name}"

    get_dagster_logger().info(f"Datagraph value: {GLEANERIO_DATAGRAPH_ENDPOINT}")
    get_dagster_logger().info(f"Provgraph value: {GLEANERIO_PROVGRAPH_ENDPOINT}")

    run_container_context = DockerContainerContext.create_for_run(
        context.dagster_run,
        context.instance.run_launcher
        if isinstance(context.instance.run_launcher, DockerRunLauncher)
        else None,
    )
    validate_docker_image(image_name)

    # Create a service var at the beginning of the function so we can check against
    # it during cleanup to see if the service was created.
    service: Optional[docker.models.services.Service] = None
    try:
        op_container_context = DockerContainerContext(
            networks=["dagster_network"],
            container_kwargs={
                "working_dir": "/opt/dagster/app",
            },
        )
        # We merge the two contexts to get the parent env vars in the child
        container_context = run_container_context.merge(op_container_context)

        get_dagster_logger().info(
            f"Spinning up {container_name=} with {image_name=}, and {args=}"
        )
        service, container = create_service(
            docker.DockerClient(),
            container_context,
            image=image_name,
            command=args,
            name=container_name,
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

        s3_client = S3()
        s3_client.load(
            data=str(logs).encode(),
            remote_path=f"scheduler/logs/{container_name}_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.log",
        )

        get_dagster_logger().info("Sent container Logs to s3: ")

        if exit_status != 0:
            raise Exception(
                f"{container_name} returned exit code '{exit_status}'. See logs in S3"
            )
    finally:
        if service:
            service.remove()
            get_dagster_logger().info(f"Removed Service: {service.name}")


def slack_error_fn(context: RunFailureSensorContext) -> str:
    get_dagster_logger().info("Sending notification to Slack")
    # The make_slack_on_run_failure_sensor automatically sends the job
    # id and name so you can just send the error. We don't need other data in the string
    source_being_crawled = context.partition_key
    if source_being_crawled:
        return f"Error for partition: {source_being_crawled}: {context.failure_event.message}"
    else:
        return f"Error: {context.failure_event.message}"


def template_rclone(input_template_file_path: str) -> str:
    """Fill in a template with shared env vars and return the templated data"""
    vars_in_rclone_config = {
        var: strict_env(var)
        for var in [
            "LAKEFS_ENDPOINT_URL",
            "LAKEFS_ACCESS_KEY_ID",
            "LAKEFS_SECRET_ACCESS_KEY",
            "GLEANERIO_MINIO_ADDRESS",
            "GLEANERIO_MINIO_PORT",
            "GLEANERIO_MINIO_USE_SSL",
            "GLEANERIO_MINIO_BUCKET",
            "MINIO_SECRET_KEY",
            "MINIO_ACCESS_KEY",
        ]
    }
    env = Environment(
        loader=FileSystemLoader(os.path.dirname(input_template_file_path)),
        undefined=jinja2.StrictUndefined,
    )
    template = env.get_template(os.path.basename(input_template_file_path))

    # Render the template with the context
    return template.render(**vars_in_rclone_config)


def template_gleaner_or_nabu(input_template_file_path: str) -> str:
    """Fill in a template with shared env vars and return the templated data"""
    vars_in_both_nabu_and_gleaner_configs = {
        var: strict_env(var)
        for var in [
            "GLEANERIO_MINIO_ADDRESS",
            "MINIO_ACCESS_KEY",
            "MINIO_SECRET_KEY",
            "GLEANERIO_MINIO_BUCKET",
            "GLEANERIO_MINIO_PORT",
            "GLEANERIO_MINIO_USE_SSL",
            "GLEANERIO_DATAGRAPH_ENDPOINT",
            "GLEANERIO_GRAPH_URL",
            "GLEANERIO_PROVGRAPH_ENDPOINT",
            "GLEANERIO_MINIO_REGION",
            "GLEANER_HEADLESS_ENDPOINT",
        ]
    }

    env = Environment(
        loader=FileSystemLoader(os.path.dirname(input_template_file_path)),
        undefined=jinja2.StrictUndefined,
    )
    template = env.get_template(os.path.basename(input_template_file_path))

    # Render the template with the context
    return template.render(**vars_in_both_nabu_and_gleaner_configs)


def all_dependencies_materialized(
    context: OpExecutionContext, dependency_asset_key: str
) -> bool:
    """Check if all partitions of a given asset are materialized"""
    instance = context.instance
    all_partitions = sources_partitions_def.get_partition_keys(
        dynamic_partitions_store=instance
    )
    # Check if all partitions of finished_individual_crawl are materialized
    materialized_partitions = context.instance.get_materialized_partitions(
        asset_key=AssetKey(dependency_asset_key)
    )

    if len(all_partitions) != len(materialized_partitions):
        get_dagster_logger().warning(
            f"Not all partitions of {dependency_asset_key} are materialized, so nq generation will be skipped"
        )
        return False
    else:
        get_dagster_logger().info(
            f"All partitions of {dependency_asset_key} are detected as having been materialized"
        )
        return True
