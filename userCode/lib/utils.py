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
import docker
import docker.errors
import docker.models
import docker.models.containers
import docker.models.services
from jinja2 import Environment, FileSystemLoader
import jinja2
from .dagster_helpers import (
    dagster_log_with_parsed_level,
    sources_partitions_def,
)
from .classes import S3
from .types import cli_modes
from .env import (
    GLEANERIO_DATAGRAPH_ENDPOINT,
    GLEANERIO_PROVGRAPH_ENDPOINT,
    RUNNING_AS_TEST_OR_DEV,
    strict_env,
    strict_env_int,
)
from dagster_docker.utils import validate_docker_image


def remove_non_alphanumeric(string: str):
    return re.sub(r"[^a-zA-Z0-9_]+", "", string)


def create_max_length_container_name(source: str, action_name: str):
    """Docker containers cannot be named longer than 63 characters; we trim the source if it is very long and would cause an issue"""
    MAX_DOCKER_CONTAINER_NAME = 63

    charsToUse = MAX_DOCKER_CONTAINER_NAME - len(source)
    if len(action_name) > charsToUse:
        action_name = action_name[:charsToUse]

    result = f"{source}_{action_name}"
    assert len(result) <= MAX_DOCKER_CONTAINER_NAME
    return result


def run_scheduler_docker_image(
    source: str,  # which organization we are crawling
    image_name: str,  # the name of the docker image to pull and validate
    args: list[str],  # the list of arguments to pass to the gleaner/nabu command
    action_name: cli_modes,  # the name of the action to run inside gleaner/nabu
    volumeMapping: Optional[list[str]] = None,
):
    """Run a docker image inside the dagster docker runtime"""
    container_name = create_max_length_container_name(source, action_name)

    get_dagster_logger().info(f"Datagraph value: {GLEANERIO_DATAGRAPH_ENDPOINT}")
    get_dagster_logger().info(f"Provgraph value: {GLEANERIO_PROVGRAPH_ENDPOINT}")

    validate_docker_image(image_name)

    client = docker.DockerClient()

    if volumeMapping:
        for volume in volumeMapping:
            src = volume.split(":")[0]
            assert os.path.exists(src), f"volume {src} does not exist"
            if src.endswith("/"):
                assert os.path.isdir(src), f"volume {src} is not a directory"
            else:
                assert os.path.isfile(src), f"volume {src} is not a file"

    container = client.containers.run(
        image_name,
        name=container_name,
        command=args,
        detach=True,  # run the container in the background non-blocking so we can stream logs
        # We do not auto remove the container so we can explicitly remove it ourselves
        auto_remove=False,
        remove=False,
        # we need to set the dagster network so it can communicate with minio/graphdb even though it is outside the compose project
        network="dagster_network",
        volumes=volumeMapping,
    )

    get_dagster_logger().info(
        f"Spinning up {container_name=} with {image_name=}, and {args=}"
    )

    logBuffer = ""

    try:
        for line in container.logs(stdout=True, stderr=True, stream=True, follow=True):
            decoded = line.decode("utf-8")
            dagster_log_with_parsed_level(decoded)
            logBuffer += decoded

        exit_status: int = container.wait()["StatusCode"]
        get_dagster_logger().info(f"Container Wait Exit status:  {exit_status}")
    finally:
        container.stop()
        container.remove()

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
            "GLEANERIO_MINIO_ADDRESS",
            "GLEANERIO_MINIO_PORT",
            "GLEANERIO_MINIO_USE_SSL",
            "GLEANERIO_MINIO_BUCKET",
            "MINIO_SECRET_KEY",
            "MINIO_ACCESS_KEY",
        ]
    }
    if RUNNING_AS_TEST_OR_DEV():
        vars_in_rclone_config["GLEANERIO_MINIO_ADDRESS"] = "localhost"

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
