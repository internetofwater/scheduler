import os
import shlex
import shutil
import subprocess
import sys
import argparse

BUILD_DIR = os.path.join(os.path.dirname(__file__), "build")
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")

"""
This file is the CLI for managing the docker swarm stack.
You should not need to run any docker commands directly if you are using this CLI.
"""


def run_subprocess(command: str, returnStdoutInsteadOfPrint: bool = False):
    """Run a shell command and stream the output in realtime"""
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE if returnStdoutInsteadOfPrint else sys.stdout,
        stderr=sys.stderr,
    )
    stdout, _ = process.communicate()
    if process.returncode != 0:
        sys.exit(process.returncode)

    return stdout.decode("utf-8") if returnStdoutInsteadOfPrint else None


def login():
    """Log into the user code container"""

    # Get the name of the user code container
    containerName = run_subprocess(
        "docker ps --filter name=geoconnex_crawler_dagster_user_code --format '{{.Names}}'",
        returnStdoutInsteadOfPrint=True,
    )
    if not containerName:
        raise RuntimeError("Could not find the user code container to log in")
    containerName = containerName.strip()  # Remove extra newline characters

    # Start an interactive shell session in the container
    run_subprocess(f"docker exec -it {containerName} /bin/bash")


def down():
    """Stop the docker swarm stack"""
    run_subprocess("docker swarm leave --force || true")


def up(local: bool, debug: bool):
    """Run the docker swarm stack"""

    if not os.path.exists(".env"):
        # check if you are running in a terminal or in CI/CD
        if not sys.stdin.isatty():
            shutil.copy(".env.example", ".env")
        else:
            answer = input(
                "Missing .env file. Do you want to copy .env.example to .env ? (y/n)"
            ).lower()
            if answer == "y" or answer == "yes":
                print("Copying .env.example to .env")
                shutil.copy(".env.example", ".env")
            else:
                print("Missing .env file. Exiting")
                return

    # Reset the swarm if it exists
    run_subprocess("docker swarm leave --force || true")
    run_subprocess("docker swarm init")

    # Create a network that we can attach to from the swarm
    run_subprocess(
        "docker network create --driver overlay --attachable dagster_network"
    )

    # Needed for a docker issue on MacOS; sometimes this dir isn't present
    os.makedirs("/tmp/io_manager_storage", exist_ok=True)
    run_subprocess(
        f"docker build -t dagster_user_code_image -f ./Docker/Dockerfile_user_code . --build-arg DAGSTER_DEBUG={'true' if debug else 'false'}"
    )

    run_subprocess(
        "docker build -t dagster_webserver_image -f ./Docker/Dockerfile_dagster ."
    )
    run_subprocess(
        "docker build -t dagster_daemon_image -f ./Docker/Dockerfile_dagster ."
    )

    if local and not debug:
        compose_args = "-c ./Docker/docker-compose-user-code.yaml -c ./Docker/docker-compose-local.yaml -c ./Docker/docker-compose-separated-dagster.yaml"
    elif local and debug:
        os.environ["DAGSTER_DEBUG_UI"] = "3000:3000"
        os.environ["DAGSTER_DEBUGPY_PORT"] = "5678:5678"
        compose_args = "-c ./Docker/docker-compose-user-code.yaml -c ./Docker/docker-compose-local.yaml -c ./Docker/docker-compose-debug.yaml"
    else:
        compose_args = "-c ./Docker/docker-compose-user-code.yaml -c ./Docker/docker-compose-separated-dagster.yaml"

    run_subprocess(
        f"docker stack deploy {compose_args} geoconnex_crawler --detach=false"
    )


def refresh():
    """Rebuild the user code for dagster, but not anything else"""

    # Rebuild the user code Docker image
    run_subprocess(
        "docker build -t dagster_user_code_image -f ./Docker/Dockerfile_user_code ."
    )

    # Update the running service with the new image
    run_subprocess(
        "docker service update --image dagster_user_code_image geoconnex_crawler_dagster_user_code"
    )


def test(*args):
    """Run pytest inside the user code container with optional arguments"""

    # Get the name of the container
    containerName = run_subprocess(
        "docker ps --filter name=geoconnex_crawler_dagster_user_code --format '{{.Names}}'",
        returnStdoutInsteadOfPrint=True,
    )
    if not containerName:
        raise RuntimeError("Could not find the user code container to run pytest")
    containerName = containerName.strip()

    # Properly quote arguments to preserve double quotes. pytest requires double quotes around marks
    # in order to work properly. i.e. -m "not requires_secret"
    quoted_args = " ".join(
        shlex.quote(arg) if " " not in arg else f'"{arg}"' for arg in args
    )

    pytest_command = f"pytest userCode/ -vvvxs {quoted_args}"

    print(pytest_command)
    if not sys.stdin.isatty():
        run_subprocess(f'docker exec {containerName} "{pytest_command}"')
    else:
        run_subprocess(f"docker exec -it {containerName} {pytest_command}")


def main():
    # set DOCKER_CLI_HINTS false to avoid the advertisement message after every docker cmd
    os.environ["DOCKER_CLI_HINTS"] = "false"

    # make sure the user is in the same directory as this file
    file_dir = os.path.dirname(os.path.abspath(__file__))
    if file_dir != os.getcwd():
        raise RuntimeError(
            "Must run from same directory as the cli in order for paths to be correct"
        )

    parser = argparse.ArgumentParser(description="Docker Swarm Stack Management")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("down", help="Stop the docker swarm stack")

    local_parser = subparsers.add_parser(
        "local", help="Spin up the docker swarm stack with local s3 and graphdb"
    )
    local_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enables a debugger for Dagster. Requires the vscode debugger to be running.",
    )

    subparsers.add_parser(
        "refresh",
        help="Rebuild and reload the user code for dagster without touching other services",
    )

    subparsers.add_parser(
        "prod",
        help="Spin up the docker swarm stack with remote s3 and graphdb",
    )

    test_parser = subparsers.add_parser(
        "test",
        help="Run pytest inside the user code container. Pass additional pytest arguments as needed",
    )
    test_parser.add_argument(
        "pytest_args",
        nargs="*",
        help="Additional arguments to pass to pytest (e.g., -- -k 'special_fn')",
    )

    subparsers.add_parser(
        "login", help="Log into the user code container (interactive shell)"
    )

    args = parser.parse_args()
    if args.command == "down":
        down()
    elif args.command == "local":
        up(local=True, debug=args.debug)
    elif args.command == "prod":
        up(local=False, debug=False)
    elif args.command == "refresh":
        refresh()
    elif args.command == "test":
        test(*args.pytest_args)
    elif args.command == "login":
        login()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
