import os
import shutil
from typing import Annotated
from dotenv import load_dotenv
import typer
import sys
from cli_lib.utils import (
    strict_env,
    run_command,
)

app = typer.Typer()

BUILD_DIR = os.path.join(os.path.dirname(__file__), "build")
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")


def run_docker_stack():
    """Initialize and run the docker swarm stack"""

    # Reset the swarm if it exists
    run_command("docker swarm leave --force || true", print_output=False)
    run_command("docker swarm init")

    # Create a network that we can attach to from the swarm
    run_command("docker network create --driver overlay --attachable dagster_network")

    network_name = strict_env("GLEANERIO_HEADLESS_NETWORK")
    network_list = run_command("docker network ls", print_output=False).stdout
    swarm_state = run_command(
        "docker info --format '{{.Swarm.LocalNodeState}}'", print_output=False
    ).stdout.strip()

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
                run_command(
                    f"docker network create -d bridge --attachable {network_name}"
                ).returncode
                == 0
            ):
                print(f"Created network {network_name}")
            else:
                print("ERROR: *** Failed to create local network.")
                sys.exit(1)
        else:
            if (
                run_command(
                    f"docker network create -d overlay --attachable {network_name}"
                ).returncode
                == 0
            ):
                print(f"Created network {network_name}")
            else:
                print("ERROR: *** Failed to create swarm network.")
                sys.exit(1)

    # Needed for a docker issue on MacOS; sometimes this dir isn't present
    os.makedirs("/tmp/io_manager_storage", exist_ok=True)

    # Build then deploy the docker swarm stack
    run_command(
        "docker build -t dagster_user_code_image -f ./Docker/Dockerfile_user_code ."
    )
    run_command(
        "docker build -t dagster_webserver_image -f ./Docker/Dockerfile_dagster ."
    )
    run_command("docker build -t dagster_daemon_image -f ./Docker/Dockerfile_dagster .")
    run_command(
        "docker stack deploy -c docker-compose-swarm.yaml geoconnex_crawler --detach=false"
    )



@app.command()
def down():
    """Stop the docker swarm stack"""
    run_command("docker swarm leave --force || true", print_output=True)


@app.command()
def up(
    env: Annotated[str, typer.Option(help="File containing your env vars")] = ".env",
):
    """Generate all config files and run the docker swarm stack"""

    if not os.path.exists(env):
        print("Missing .env file. Do you want to copy .env.example to .env ? (y/n)")
        answer = input().lower()
        if answer.lower() == "y" or answer.lower() == "yes":
            shutil.copy(".env.example", ".env")
        else:
            print("Missing .env file. Exiting")
            return

    load_dotenv(env)
    run_command("docker swarm leave --force || true", print_output=False)
    run_docker_stack()


@app.command()
def env(
    env: Annotated[str, typer.Option(help="File containing your env vars")] = ".env",
):
    """Print the env vars that will be used for the docker swarm stack"""
    load_dotenv(env)
    for key, value in os.environ.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    app()
