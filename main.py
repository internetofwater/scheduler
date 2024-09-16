import os
import re
from typing import Annotated
from bs4 import BeautifulSoup
import requests
import typer
from jinja2 import Environment, FileSystemLoader
import yaml

app = typer.Typer()

BUILD_DIR =    os.path.join(os.path.dirname(__file__), "build")
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")

def remove_non_alphanumeric(string):
    return re.sub(r'[^a-zA-Z0-9_]+', '', string)

def strict_env(env_var: str) -> str:
    val = os.environ.get(env_var)
    if val is None:
        raise RuntimeError(f"Missing required environment variable: {env_var}")
    return val

def read_common_env():
    return {
        "GLEANERIO_MINIO_ADDRESS": strict_env("GLEANERIO_MINIO_ADDRESS"),
        "GLEANERIO_MINIO_ACCESS_KEY": strict_env("GLEANERIO_MINIO_ACCESS_KEY"),
        "GLEANERIO_MINIO_SECRET_KEY": strict_env("GLEANERIO_MINIO_SECRET_KEY"),
        "GLEANERIO_MINIO_BUCKET": strict_env("GLEANERIO_MINIO_BUCKET"),
        "GLEANERIO_MINIO_PORT": strict_env("GLEANERIO_MINIO_PORT"),
        "GLEANERIO_MINIO_USE_SSL": strict_env("GLEANERIO_MINIO_USE_SSL"),
    }

def template_config(base, out_dir):
    env = Environment(loader=FileSystemLoader(os.path.dirname(base)))
    template = env.get_template(os.path.basename(base)) 

    # Render the template with the context
    rendered_content = template.render(**read_common_env())

    # Write the rendered content to the output file
    output_name = str(os.path.basename(base)).removesuffix('.j2')

    with open(os.path.join(out_dir, output_name), 'w+') as file:
        file.write(rendered_content)

    return os.path.join(out_dir, output_name)

    
@app.command()
def generate_gleaner_config(sitemap_url: Annotated[str, typer.Option()] = "https://geoconnex.us/sitemap.xml",
                base: Annotated[str, typer.Option(help="nabu config to use as source")] = os.path.join(TEMPLATE_DIR, "gleanerconfigPREFIX.yaml.j2"),
    out_dir: Annotated[str, typer.Option(help="Directory for output")] = BUILD_DIR
            ):
    """Generate the gleaner config from a remote sitemap"""

    # Fill in the config with the common minio configuration
    common_config = template_config(base, out_dir)

    with open(common_config, 'r') as base_file:
        base_data = yaml.safe_load(base_file)

    # Parse the sitemap INDEX for the referenced sitemaps for a config file
    Lines = []

    r = requests.get(sitemap_url)
    xml = r.text
    soup = BeautifulSoup(xml, features='xml')
    sitemapTags = soup.find_all("sitemap")

    for sitemap in sitemapTags:
        Lines.append(sitemap.findNext("loc").text)  # type: ignore

    sources = []
    names = set()
    for line in Lines:
        data = {}

        basename = sitemap_url.removesuffix(".xml")
        name =  line.removeprefix(basename).removesuffix(".xml").removeprefix("/").removesuffix("/").replace("/", "_")
        name = remove_non_alphanumeric(name)
        
        data["sourcetype"] = "sitemap"
        
        if name in names:
            print(f"Warning! Skipping duplicate name {name}")
            continue
        
        names.add(name)
        data["name"] = name
        data["url"] = line.strip()
        data["headless"] = "false"
        data["pid"] = "https://gleaner.io/genid/geoconnex"
        data["propername"] = name
        data["domain"] = "https://geoconnex.us"
        data["active"] = "true"

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
def all():
    """Generate all the files """
    clean()
    generate_gleaner_config()
    generate_nabu_config()

@app.command()
def clean():
    """Delete the contents of the build directory"""

    if os.path.exists(BUILD_DIR):
        print("Cleaning contents of the build directory")
        
        for root, _, files in os.walk(BUILD_DIR):
            for name in files:
                if name != '.gitkeep' and name != "__init__.py":
                    os.remove(os.path.join(root, name))
    else:
        print("Build directory does not exist")

if __name__ == "__main__":
    app()