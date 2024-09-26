from collections import namedtuple
import os
import pty
import shutil
import subprocess
from typing import Annotated
from dotenv import load_dotenv
import typer
import sys


def strict_env(env_var: str) -> str:
    val = os.environ.get(env_var)
    if val is None:
        raise RuntimeError(f"Missing required environment variable: {env_var}")
    return val


CommandResult = namedtuple("CommandResult", ["stdout", "stderr", "returncode"])


def run_command(command: str, print_output: bool = True) -> CommandResult:
    """Given a command string, run it and display the results in the console in realtime"""
    master_fd, slave_fd = pty.openpty()

    process = subprocess.Popen(
        command,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True,
        bufsize=1,
        universal_newlines=True,
    )

    os.close(slave_fd)

    stdout = []
    stderr = []

    def read_fd(fd, output_list, stream_name):
        while True:
            data = os.read(fd, 1024)
            if not data:
                break
            output_list.append(data.decode())
            if print_output:
                print(f"[{stream_name}] {data.decode()}", end="")

    stdout_fd = process.stdout.fileno() if process.stdout is not None else None
    stderr_fd = process.stderr.fileno() if process.stderr is not None else None

    while process.poll() is None:
        read_fd(stdout_fd, stdout, "stdout")
        read_fd(stderr_fd, stderr, "stderr")

    # Read any remaining output after the process ends
    read_fd(stdout_fd, stdout, "stdout")
    read_fd(stderr_fd, stderr, "stderr")

    # Close the file descriptors
    if stdout_fd is not None:
        os.close(stdout_fd)
    if stderr_fd is not None:
        os.close(stderr_fd)

    return CommandResult("".join(stdout), "".join(stderr), process.returncode)


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
