import os
import shlex
import shutil
import subprocess
import sys
import argparse

BUILD_DIR = os.path.join(os.path.dirname(__file__), "build")
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")

"""
This file is the CLI for managing Docker Compose-based infrastructure.
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
    """Stop the Docker Compose services"""
    run_subprocess(
        "docker compose --profile localInfra --profile dagsterStorage -f Docker/Docker-compose.yaml down"
    )


def up():
    """Run the Docker Compose services"""

    if not os.path.exists(".env"):
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

    # Build the images
    run_subprocess(
        "docker compose --profile localInfra --profile dagsterStorage -f Docker/Docker-compose.yaml up"
    )


def refresh():
    """Rebuild the user code for Dagster, but not anything else"""
    run_subprocess(
        "docker compose --profile localInfra --profile dagsterStorage -f Docker/Docker-compose.yaml build geoconnex_crawler_dagster_user_code"
    )


def test(*args):
    """Run pytest inside the user code container with optional arguments"""
    containerName = run_subprocess(
        "docker ps --filter name=geoconnex_crawler_dagster_user_code --format '{{.Names}}'",
        returnStdoutInsteadOfPrint=True,
    )
    if not containerName:
        raise RuntimeError("Could not find the user code container to run pytest")
    containerName = containerName.strip()

    quoted_args = " ".join(
        shlex.quote(arg) if " " not in arg else f'"{arg}"' for arg in args
    )

    pytest_command = f"pytest userCode/ -vvvxs {quoted_args}"

    print(f"Running pytest cmd: '{pytest_command}'")
    if not sys.stdin.isatty():
        run_subprocess(f"docker exec {containerName} {pytest_command}")
    else:
        run_subprocess(f"docker exec -it {containerName} {pytest_command}")


def main():
    # Set DOCKER_CLI_HINTS false to avoid the advertisement message after every docker cmd
    os.environ["DOCKER_CLI_HINTS"] = "false"

    # Make sure the user is in the same directory as this file
    file_dir = os.path.dirname(os.path.abspath(__file__))
    if file_dir != os.getcwd():
        raise RuntimeError(
            "Must run from same directory as the CLI in order for paths to be correct"
        )

    parser = argparse.ArgumentParser(description="Docker Compose Management")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("down", help="Stop the Docker Compose services")

    local_parser = subparsers.add_parser(
        "local", help="Spin up the Docker Compose services with local s3 and graphdb"
    )
    local_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enables a debugger for Dagster. Requires the vscode debugger to be running.",
    )

    subparsers.add_parser(
        "refresh",
        help="Rebuild and reload the user code for Dagster without touching other services",
    )

    subparsers.add_parser(
        "prod",
        help="Spin up the Docker Compose services with remote s3 and graphdb",
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
