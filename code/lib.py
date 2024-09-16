from typing import List, Tuple

from dagster import get_dagster_logger
from dagster_docker import DockerRunLauncher
from .env import GLEANERIO_DATAGRAPH_ENDPOINT, GLEANERIO_GLEANER_IMAGE, GLEANERIO_GLEANER_CONFIG_PATH, GLEANERIO_NABU_IMAGE, GLEANERIO_NABU_CONFIG_PATH, GLEANERIO_PROVGRAPH_ENDPOINT
from .types import cli_modes

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