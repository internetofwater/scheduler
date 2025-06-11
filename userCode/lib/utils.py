# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
import os
import re

from dagster import (
    get_dagster_logger,
)
from dagster_docker.utils import validate_docker_image
import docker
import jinja2
from jinja2 import Environment, FileSystemLoader

from .classes import S3
from .dagster import (
    dagster_log_with_parsed_level,
)
from .env import (
    DATAGRAPH_REPOSITORY,
    PROVGRAPH_REPOSITORY,
    RUNNING_AS_TEST_OR_DEV,
    strict_env,
)
from .types import cli_modes


def remove_non_alphanumeric(string: str):
    return re.sub(r"[^a-zA-Z0-9_]+", "", string)


def create_max_length_container_name(source: str, action_name: str):
    """Docker containers cannot be named longer than 63 characters; we trim the source if it is very long and would cause an issue"""
    MAX_DOCKER_CONTAINER_NAME = 63

    charsToUse = MAX_DOCKER_CONTAINER_NAME - 1 - len(source)
    if len(action_name) > charsToUse:
        action_name = action_name[:charsToUse]

    result = f"{source}_{action_name}"
    assert len(result) <= MAX_DOCKER_CONTAINER_NAME, (
        f"Got container name of size, {len(result)}"
    )
    return result


def run_docker_image(
    source: str,  # which organization we are crawling
    image_name: str,  # the name of the docker image to pull and validate
    args: str,  # the list of arguments to pass to the gleaner/nabu command
    action_name: cli_modes,  # the name of the action to run inside gleaner/nabu
    exit_3_is_fatal: bool = False,
    volumeMapping: list[str] | None = None,
):
    """Run a docker using the same docker socket inside dagster"""
    container_name = create_max_length_container_name(source, action_name)

    get_dagster_logger().info(f"Datagraph value: {DATAGRAPH_REPOSITORY}")
    get_dagster_logger().info(f"Provgraph value: {PROVGRAPH_REPOSITORY}")

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

    match exit_status:
        case 0:
            pass
        case 3 if not exit_3_is_fatal:
            get_dagster_logger().warning("Harvest failed with non fatal error")
        case _:
            raise Exception(
                f"{container_name} failed with non-zero exit code '{exit_status}'. See logs in S3"
            )


def template_rclone(input_template_file_path: str) -> str:
    """Fill in a template with shared env vars and return the templated data"""
    vars_in_rclone_config = {
        var: strict_env(var)
        for var in [
            "LAKEFS_ENDPOINT_URL",
            "LAKEFS_ACCESS_KEY_ID",
            "LAKEFS_SECRET_ACCESS_KEY",
            "S3_ACCESS_KEY",
            "S3_SECRET_KEY",
            "S3_ADDRESS",
            "S3_PORT",
            "S3_REGION",
        ]
    }
    if RUNNING_AS_TEST_OR_DEV():
        vars_in_rclone_config["S3_ADDRESS"] = "localhost"

    env = Environment(
        loader=FileSystemLoader(os.path.dirname(input_template_file_path)),
        undefined=jinja2.StrictUndefined,
    )
    template = env.get_template(os.path.basename(input_template_file_path))

    # Render the template with the context
    return template.render(**vars_in_rclone_config)
