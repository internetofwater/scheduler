import os
import shutil
import subprocess
import sys
import argparse

BUILD_DIR = os.path.join(os.path.dirname(__file__), "build")
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")


def run_subprocess(command: str):
    process = subprocess.Popen(
        command, shell=True, stdout=sys.stdout, stderr=sys.stderr
    )
    process.communicate()
    return process.returncode


def down():
    """Stop the docker swarm stack"""
    run_subprocess("docker swarm leave --force || true")


def up(env: str = ".env"):
    """Generate all config files and run the docker swarm stack"""

    if not os.path.exists(env):
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

    # hard code this so we don't need to use dotenv to load it
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

    # Build then deploy the docker swarm stack
    run_subprocess(
        "docker build -t dagster_user_code_image -f ./Docker/Dockerfile_user_code ."
    )
    run_subprocess(
        "docker build -t dagster_webserver_image -f ./Docker/Dockerfile_dagster ."
    )
    run_subprocess(
        "docker build -t dagster_daemon_image -f ./Docker/Dockerfile_dagster ."
    )
    run_subprocess(
        "docker stack deploy -c docker-compose-swarm.yaml geoconnex_crawler --detach=false"
    )


def main():
    parser = argparse.ArgumentParser(description="Docker Swarm Stack Management")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("down", help="Stop the docker swarm stack")

    up_parser = subparsers.add_parser(
        "up", help="Generate all config files and run the docker swarm stack"
    )
    up_parser.add_argument(
        "--env", type=str, default=".env", help="File containing your env vars"
    )

    args = parser.parse_args()
    if args.command == "down":
        down()
    elif args.command == "up":
        up(args.env)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
