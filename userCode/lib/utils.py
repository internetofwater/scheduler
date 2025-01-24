from datetime import datetime
import os
import re
from typing import Optional, Union
from dagster import (
    AssetKey,
    OpExecutionContext,
    RunFailureSensorContext,
    get_dagster_logger,
)
from dagster._core.utils import parse_env_var
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
    strict_env_int,
)
from dagster_docker.container_context import DockerContainerContext
from dagster_docker.utils import validate_docker_image


def remove_non_alphanumeric(string):
    return re.sub(r"[^a-zA-Z0-9_]+", "", string)


def run_scheduler_docker_image(
    context: OpExecutionContext,
    source: str,  # which organization we are crawling
    image_name: str,  # the name of the docker image to pull and validate
    args: list[str],  # the list of arguments to pass to the gleaner/nabu command
    action_name: str,  # the name of the action to run inside gleaner/nabu
    volumeMapping: Optional[list[str]] = None,
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

    op_container_context = DockerContainerContext(
        container_kwargs={
            "working_dir": "/opt/dagster/app",
        },
    )
    # We merge the two contexts to get the parent env vars in the child
    container_context = run_container_context.merge(op_container_context)

    client = docker.DockerClient()
    env_vars = dict([parse_env_var(env_var) for env_var in container_context.env_vars])

    container = client.containers.run(
        image_name,
        args,
        name=container_name,
        detach=True,  # run the container in the background non-blocking so we can stream logs
        environment=env_vars,
        auto_remove=True,
        remove=True,
        volumes=volumeMapping,
    )

    get_dagster_logger().info(
        f"Spinning up {container_name=} with {image_name=}, and {args=}"
    )

    logBuffer = ""

    for line in container.logs(stdout=True, stderr=True, stream=True, follow=True):
        get_dagster_logger().debug(line)
        logBuffer += line.decode("utf-8")

    exit_status: int = container.wait()["StatusCode"]
    get_dagster_logger().info(f"Container Wait Exit status:  {exit_status}")

    s3_client = S3()
    s3_client.load(
        data=str(logBuffer).encode(),
        remote_path=f"scheduler/logs/{container_name}_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.log",
    )

    get_dagster_logger().info("Sent container Logs to s3")

    if exit_status != 0:
        raise Exception(
            f"{container_name} failed with non-zero exit code '{exit_status}'. See logs in S3"
        )


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
            "SCHEDULER_MINIO_ADDRESS",
            "SCHEDULER_MINIO_PORT",
            "SCHEDULER_MINIO_USE_SSL",
            "SCHEDULER_MINIO_BUCKET",
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
    vars_in_both_nabu_and_gleaner_configs: dict[str, Union[str, int]] = {
        var: strict_env(var)
        for var in [
            "SCHEDULER_MINIO_ADDRESS",
            "MINIO_ACCESS_KEY",
            "MINIO_SECRET_KEY",
            "SCHEDULER_MINIO_BUCKET",
            "SCHEDULER_MINIO_PORT",
            "SCHEDULER_MINIO_USE_SSL",
            "SCHEDULER_DATAGRAPH_ENDPOINT",
            "SCHEDULER_GRAPH_URL",
            "SCHEDULER_PROVGRAPH_ENDPOINT",
            "SCHEDULER_MINIO_REGION",
            "GLEANER_HEADLESS_ENDPOINT",
        ]
    }

    # certain config values should be an integer
    ints = strict_env_int("GLEANER_THREADS")
    vars_in_both_nabu_and_gleaner_configs["GLEANER_THREADS"] = ints

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
