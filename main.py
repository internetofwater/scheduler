# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import os
import shutil
import subprocess
import sys
import argparse


"""
This file is the CLI for managing Docker Compose-based infrastructure.
"""


def check_dotenv():
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
                raise RuntimeError("Missing .env file. Exiting")


def run_subprocess(command: str, returnStdoutAsValue: bool = False):
    """Run a shell command and stream the output in realtime"""
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE if returnStdoutAsValue else sys.stdout,
        stderr=sys.stderr,
    )
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        if stderr:
            print(stderr.decode("utf-8"))
        if stdout:
            print(stdout.decode("utf-8"))
        sys.exit(process.returncode)
    return stdout.decode("utf-8") if returnStdoutAsValue else None


def login():
    """Log into the user code container"""
    containerName = run_subprocess(
        "docker ps --filter name=user_code --format '{{.Names}}'",
        returnStdoutAsValue=True,
    )
    if not containerName:
        raise RuntimeError("Could not find the user code container to log in")
    containerName = containerName.strip()  # Remove extra newline characters

    # Start an interactive shell session in the container
    run_subprocess(f"docker exec -it {containerName} /bin/bash")


def execute_compose(profiles: list[str], actions: list[str]):
    """Executes command on the Docker Compose services"""
    check_dotenv()

    profileCommand = " ".join(f"--profile {profile}" for profile in profiles)

    for action in actions:
        # Run subprocess without waiting if detach is True
        command = (
            f"docker compose {profileCommand} -f Docker/Docker-compose.yaml {action}"
        )
        run_subprocess(command)


def up(profiles: list[str], build: bool = False, detach: bool = False):
    """Run the Docker Compose services"""
    check_dotenv()

    profileCommand = " ".join(f"--profile {profile}" for profile in profiles)
    command = f"docker compose {profileCommand} -f Docker/Docker-compose.yaml up"
    if "production" not in profiles:
        command = "DAGSTER_POSTGRES_HOST=0.0.0.0 " + command
    if build:
        command += " --build"
    if detach:
        command += " -d"

    # Run subprocess without waiting if detach is True
    run_subprocess(command)


def main():
    # Set DOCKER_CLI_HINTS false to avoid the advertisement message after every docker cmd
    os.environ["DOCKER_CLI_HINTS"] = "false"

    parser = argparse.ArgumentParser(description="Docker Compose Management")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("down", help="Stop the Docker Compose services")

    subparsers.add_parser(
        "dagster-dev", help="Run dagster dev; will point to local dev infrastructure"
    )

    pull = subparsers.add_parser("pull", help="Pull the Docker Compose services")
    pull.add_argument(
        "--profiles",
        nargs="+",
        choices=["localInfra", "production"],
        required=True,
        help="Specify the profiles to pull",
    )

    build = subparsers.add_parser("build", help="Build the Docker Compose services")
    build.add_argument(
        "--profiles",
        nargs="+",
        choices=["localInfra", "production"],
        required=True,
        help="Specify the profiles to build",
    )

    dev = subparsers.add_parser(
        "dev", help="Run local infrastructure needed for dagster dev"
    )
    dev.add_argument(
        "--build",
        action="store_true",
        help="Build the Docker Compose services before starting",
    )

    prod = subparsers.add_parser(
        "prod", help="Run the Docker Compose services in production mode"
    )
    prod.add_argument(
        "--local-services",
        action="store_true",
        help="Spin up local infrastructure instead of managed ones.",
    )

    prod.add_argument(
        "--build",
        action="store_true",
        help="Build the Docker Compose services before starting",
    )

    dev.add_argument(
        "--detach",
        action="store_true",
        help="Run the Docker Compose services in detached mode",
    )

    prod.add_argument(
        "--detach",
        action="store_true",
        help="Run the Docker Compose services in detached mode",
    )

    subparsers.add_parser(
        "login", help="Log into the user code container (interactive shell)"
    )

    args = parser.parse_args()

    if args.command == "down":
        run_subprocess(
            "docker compose --profile localInfra --profile production -f Docker/Docker-compose.yaml down"
        )
    elif args.command == "dagster-dev":
        run_subprocess("DAGSTER_POSTGRES_HOST=0.0.0.0 dagster dev")
    elif args.command == "build":
        execute_compose(profiles=args.profiles, actions=["build"])
    elif args.command == "pull":
        execute_compose(profiles=args.profiles, actions=["pull"])
    elif args.command == "dev":
        up(profiles=["localInfra"], build=args.build, detach=args.detach)
    elif args.command == "prod":
        profiles = ["production"]
        if args.local_services:
            profiles.append("localInfra")
        up(profiles, build=args.build, detach=args.detach)
    elif args.command == "login":
        login()


if __name__ == "__main__":
    assert os.path.dirname(os.path.abspath(__file__)) == os.getcwd(), (
        "Please run this script from the root of the repository"
    )
    os.makedirs("/tmp/geoconnex", exist_ok=True)
    main()
