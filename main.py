import os
import shutil
import subprocess
import sys
import argparse

BUILD_DIR = os.path.join(os.path.dirname(__file__), "build")
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")


def run_subprocess(command: str):
    """Run a shell command and stream the output in realtime"""
    process = subprocess.Popen(
        command, shell=True, stdout=sys.stdout, stderr=sys.stderr
    )
    process.communicate()
    return process.returncode


def down():
    """Stop the docker swarm stack"""
    run_subprocess("docker swarm leave --force || true")


def up(local: bool, debug: bool):
    """Run the docker swarm stack"""

    if not os.path.exists(".env"):
        print("Missing .env file. Do you want to copy .env.example to .env ? (y/n)")
        answer = input().lower()
        if answer == "y" or answer == "yes":
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

    # hard code this so we don't need to use dotenv to load just one env var
    # network_name = strict_env("GLEANERIO_HEADLESS_NETWORK")
    network_name = "headless_gleanerio"

    network_list = subprocess.check_output("docker network ls", shell=True).decode(
        "utf-8"
    )
    swarm_state = (
        subprocess.check_output(
            "docker info --format '{{.Swarm.LocalNodeState}}'", shell=True
        )
        .decode("utf-8")
        .strip()
    )

    if network_name in network_list:
        print(f"{network_name} network exists")
        if swarm_state == "inactive":
            print("Network is not swarm")
        else:
            print("Network is swarm")
    else:
        print("Creating network")
        if swarm_state == "inactive":
            if (
                run_subprocess(
                    f"docker network create -d bridge --attachable {network_name}"
                )
                == 0
            ):
                print(f"Created network {network_name}")
            else:
                print("ERROR: *** Failed to create local network.")
                sys.exit(1)
        else:
            if (
                run_subprocess(
                    f"docker network create -d overlay --attachable {network_name}"
                )
                == 0
            ):
                print(f"Created network {network_name}")
            else:
                print("ERROR: *** Failed to create swarm network.")
                sys.exit(1)

    # Needed for a docker issue on MacOS; sometimes this dir isn't present
    os.makedirs("/tmp/io_manager_storage", exist_ok=True)
    return_val = run_subprocess(
        f"docker build -t dagster_user_code_image -f ./Docker/Dockerfile_user_code . --build-arg DAGSTER_DEBUG={"true" if debug else "false"}"
    )
    if return_val != 0:
        sys.exit(1)

    return_val = run_subprocess(
        "docker build -t dagster_webserver_image -f ./Docker/Dockerfile_dagster ."
    )
    if return_val != 0:
        sys.exit(1)

    return_val = run_subprocess(
        "docker build -t dagster_daemon_image -f ./Docker/Dockerfile_dagster ."
    )
    if return_val != 0:
        sys.exit(1)

    if local and not debug:
        compose_files = "-c docker-compose-user-code.yaml -c docker-compose-local.yaml -c docker-compose-separated-dagster.yaml"
    elif local and debug:
        compose_files = "-c docker-compose-user-code.yaml -c docker-compose-local.yaml"
    else:
        compose_files = (
            "-c docker-compose-user-code.yaml -c docker-compose-separated-dagster.yaml"
        )

    run_subprocess(
        f"docker stack deploy {compose_files} geoconnex_crawler --detach=false"
    )


def main():
    parser = argparse.ArgumentParser(description="Docker Swarm Stack Management")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("down", help="Stop the docker swarm stack")

    local_parser = subparsers.add_parser(
        "local", help="Spin up the docker swarm stack with local s3"
    )
    local_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enables a debugger for Dagster. Requires the vscode debugger to be running.",
    )

    subparsers.add_parser(
        "prod",
        help="Spin up the docker swarm stack without local s3; requires remote s3 service in .env",
    )
    args = parser.parse_args()
    if args.command == "down":
        down()
    elif args.command == "local":
        up(local=True, debug=args.debug)
    elif args.command == "prod":
        up(local=False, debug=False)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
