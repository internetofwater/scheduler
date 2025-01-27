import os
import shutil
import subprocess
import sys
import argparse

BUILD_DIR = os.path.join(os.path.dirname(__file__), "build")
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")

"""
This file is the CLI for managing Docker Compose-based infrastructure.
"""


def run_subprocess(command: str, returnStdoutAsValue: bool = False):
    """Run a shell command and stream the output in realtime"""
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE if returnStdoutAsValue else sys.stdout,
        stderr=sys.stderr,
    )
    stdout, _ = process.communicate()
    if process.returncode != 0:
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


def up(profiles: list[str], build: bool = False, dev_mode: bool = False):
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

    profileCommand = " ".join(f"--profile {profile}" for profile in profiles)
    command = f"docker compose {profileCommand} -f Docker/Docker-compose.yaml up"

    if dev_mode:
        run_subprocess("uv sync")
        command = "export DAGSTER_POSTGRES_HOST=0.0.0.0 && " + command
    else:
        command = "export DAGSTER_POSTGRES_HOST=dagster_postgres && " + command
    if build:
        run_subprocess(f"{command} --build")
    else:
        run_subprocess(command)


def main():
    # Set DOCKER_CLI_HINTS false to avoid the advertisement message after every docker cmd
    os.environ["DOCKER_CLI_HINTS"] = "false"

    parser = argparse.ArgumentParser(description="Docker Compose Management")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("down", help="Stop the Docker Compose services")

    dev = subparsers.add_parser("dev", help="Run dagster dev with local infrastructure")
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

    subparsers.add_parser(
        "login", help="Log into the user code container (interactive shell)"
    )

    args = parser.parse_args()

    if args.command == "down":
        run_subprocess(
            "docker compose --profile localInfra --profile production -f Docker/Docker-compose.yaml down"
        )
    elif args.command == "dev":
        up(profiles=["localInfra"], build=args.build, dev_mode=True)
    elif args.command == "prod":
        profiles = ["production"]
        if args.local_services:
            profiles.append("localInfra")
        up(profiles, build=args.build, dev_mode=False)
    elif args.command == "login":
        login()


if __name__ == "__main__":
    main()
