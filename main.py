import os
import shutil
import pty
from typing import Annotated
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import requests
import typer
import yaml
import sys
from cli_lib.utils import remove_non_alphanumeric, strict_env, template_config, run_command

app = typer.Typer()

BUILD_DIR =    os.path.join(os.path.dirname(__file__), "build")
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")

def run_docker_stack():

    # Reset the swarm if it exists
    run_command("docker swarm leave --force || true", print_output=False)
    run_command("docker swarm init")

    reload_docker_configs()

    # Create a network that we can attach to from the swarm
    run_command("docker network create --driver overlay --attachable dagster_network")

    network_name = strict_env("GLEANERIO_HEADLESS_NETWORK")
    network_list = run_command("docker network ls", print_output=False).stdout
    swarm_state = run_command("docker info --format '{{.Swarm.LocalNodeState}}'", print_output=False).stdout.strip()

    if network_name in network_list:
        print(f"{network_name} network exists")
        if swarm_state == "inactive":
            print("Network is not swarm")
        else:
            print("Network is swarm")
    else:
        print("Creating network")
        if swarm_state == "inactive":
            if run_command(f"docker network create -d bridge --attachable {network_name}").returncode == 0:
                print(f"Created network {network_name}")
            else:
                print("ERROR: *** Failed to create local network.")
                sys.exit(1)
        else:
            if run_command(f"docker network create -d overlay --attachable {network_name}").returncode == 0:
                print(f"Created network {network_name}")
            else:
                print("ERROR: *** Failed to create swarm network.")
                sys.exit(1)

    # Needed for a docker issue on MacOS; sometimes this dir isn't present
    os.makedirs("/tmp/io_manager_storage", exist_ok=True)

    # Build then deploy the docker swarm stack
    run_command("docker build -t dagster_user_code_image -f ./Docker/Dockerfile_user_code .")
    run_command("docker build -t dagster_webserver_image -f ./Docker/Dockerfile_dagster .")
    run_command("docker build -t dagster_daemon_image -f ./Docker/Dockerfile_dagster .")
    run_command("docker stack deploy -c docker-compose-swarm.yaml geoconnex_crawler --detach=false", print_output=True)

def reload_docker_configs():
    """Reload the config files present in the docker swarm"""
    config_paths = {
        "gleaner": strict_env("GLEANERIO_GLEANER_CONFIG_PATH"),
        "nabu": strict_env("GLEANERIO_NABU_CONFIG_PATH"),
    }

    for config in config_paths.values():
        run_command(f"docker config rm {config}")

    for config_name, config_path in config_paths.items():
        config_list = run_command("docker config ls", print_output=False).stdout
        if config_path in config_list:
            print(f"{config_path} config exists")
        else:
            print(f"Creating config {config_name}")
            if run_command(f"docker config create {config_name} \"{config_path}\"").returncode == 0:
                print(f"Created config {config_name} {config_path}")
            else:
                print(f"ERROR: *** Failed to create {config_name} config.")
                sys.exit(1)
    
@app.command()
def generate_gleaner_config(sitemap_url: Annotated[str, typer.Option()] = "https://geoconnex.us/sitemap.xml",
                base: Annotated[str, typer.Option(help="nabu config to use as source")] = os.path.join(TEMPLATE_DIR, "gleanerconfigPREFIX.yaml.j2"),
    out_dir: Annotated[str, typer.Option(help="Directory for output")] = BUILD_DIR
            ):
    """Generate the gleaner config from a remote sitemap"""

    # Fill in the config with the common minio configuration
    base_config = template_config(base, out_dir)

    with open(base_config, 'r') as base_file:
        base_data = yaml.safe_load(base_file)

    # Parse the sitemap index for the referenced sitemaps for a config file
    r = requests.get(sitemap_url)
    xml = r.text
    sitemapTags = BeautifulSoup(xml, features='xml').find_all("sitemap")
    Lines = [ sitemap.findNext("loc").text for sitemap in sitemapTags ]

    sources = []
    names = set()
    for line in Lines:

        basename = sitemap_url.removesuffix(".xml")
        name =  line.removeprefix(basename).removesuffix(".xml").removeprefix("/").removesuffix("/").replace("/", "_")
        name = remove_non_alphanumeric(name)
        if name in names:
            print(f"Warning! Skipping duplicate name {name}")
            continue

        data = {
            "sourcetype": "sitemap",
            "name": name,
            "url": line.strip(),
            "headless": "false",
            "pid": "https://gleaner.io/genid/geoconnex",
            "propername": name,
            "domain": "https://geoconnex.us",
            "active": "true"
        }
        names.add(name)
        sources.append(data)

    # Combine base data with the new sources
    if isinstance(base_data, dict):
        base_data["sources"] = sources
    else:
        base_data = {"sources": sources}

    # Write the combined data to the output YAML file
    with open(os.path.join(BUILD_DIR, 'gleanerconfig.yaml'), 'w') as outfile:
        yaml.dump(base_data, outfile, default_flow_style=False)

@app.command()
def generate_nabu_config(
    base: Annotated[str, typer.Option(help="nabu config to use as source")] = os.path.join(TEMPLATE_DIR, "nabuconfig.yaml.j2"),
    out_dir: Annotated[str, typer.Option(help="Directory for output")] = BUILD_DIR
):
    """Generate the nabu config from the base template""" 
    template_config(base, out_dir)


@app.command()
def stop_swarm():
    """Stop the docker swarm stack"""
    run_command("docker swarm leave --force || true", print_output=True)

@app.command()
def all(env: Annotated[str, typer.Option(help="File containing your env vars")] = ".env"):
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
    clean()
    generate_gleaner_config()
    generate_nabu_config()
    run_docker_stack()

@app.command()
def clean():
    """Delete the contents of the build directory"""
    if os.path.exists(BUILD_DIR):
        for name in os.listdir(BUILD_DIR):
            if name != '.gitkeep' and name != "__init__.py":
                os.remove(os.path.join(BUILD_DIR, name))
    else:
        print("Build directory does not exist")
        os.mkdir(BUILD_DIR)

    print("Removed contents of the build directory")

if __name__ == "__main__":
    app()
