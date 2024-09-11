import fileinput
import os
import re
import tempfile
from typing import Annotated
from bs4 import BeautifulSoup
import requests
import typer
import shutil
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


def generate_repository(config, output_dir: str):
    """Generate the topline repository python file that links to all the jobs"""
    c = yaml.safe_load(open(config, 'r'))

    # Set up Jinja2 environment and load the template
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
    template = env.get_template('repository.py.j2') 

    # Render the template with the context
    rendered_content = template.render(sources=c["sources"])

    # Write the rendered content to the output file
    with open(os.path.join(output_dir, "repository.py"), 'w+') as file:
        file.write(rendered_content)

def generate_code(cf, od, template_dir: str, days: int):
    """Generate each individual dagster job that is a separate python file"""
    c = yaml.safe_load(open(cf, 'r'))

    with tempfile.TemporaryDirectory() as tmpdirname:
        print('created temporary directory', tmpdirname)
        operations = ["ops", "jobs", "sch"]

        # Get the count and divide into the hour count range (time
        # interval) we want to sprad thm over.
        hours = int(days) * 24  # days is the cli param for the number of days to work over
        inc = round(hours / len(c["sources"])) # divide hours we want to run over by number of source to get increment

        print("index event every {} hours over {} day(s) period for {} items".format(inc, days, len(c["sources"])))

        for i, source in enumerate(c["sources"]):

            # copy the templates to the temp dir
            for operation in operations:
                src = "{0}/implnet_{1}_SOURCEVAL.py".format(template_dir, operation)
                dst = "{0}/implnet_{1}_{2}.py".format(tmpdirname, operation, source["name"])
                shutil.copyfile(src, dst)

                # update the templates loop 1
                with open(dst, 'r') as file:
                    file_contents = file.read()

                # replace the desired text in the string
                new_contents = file_contents.replace("SOURCEVAL", source["name"])

                # write the new contents back to the file
                with open(dst, 'w') as file:
                    file.write(new_contents)

                # update the templates loop 2
                if "sch" in operation:
                    di = int(days)
                    q = (((i * inc) / 24) % di) // 1
                    r = (i * inc) % 24
                    new_cron_schedule = "0 {} {} * *".format(r, int(q)+1)
                    pattern = r'cron_schedule="(.+?)"'
                    # print("index {}::  {}".format(i,new_cron_schedule))
                    for line in fileinput.input(dst, inplace=True):
                        line = re.sub(pattern, f'cron_schedule="{new_cron_schedule}"', line)
                        # line = re.sub(r'0 24 * * *', f'0 {r} * * {int(q)}', line)
                        print(line, end='')

                        # make directories in the output directory
            for operation in operations:
                if not os.path.exists("{0}/{1}".format(od, operation)):
                    os.makedirs("{0}/{1}".format(od, operation))

            # copy new files to output dir
            for operation in operations:
                src = "{0}/implnet_{1}_{2}.py".format(tmpdirname, operation, source["name"])
                dst = "{0}/{1}/implnet_{1}_{2}.py".format(od, operation, source["name"])
                shutil.copyfile(src, dst)


@app.command()
def generate_python_jobs(
    # weekly: Annotated[int, typer.Option(help="Spread the run over a week")],
    # monthly: Annotated[str, typer.Option(help="Spread the run over a month")],
    config: Annotated[str, typer.Option(help="Gleaner config to use as source")] = os.path.join(BUILD_DIR, "gleanerconfig.yaml"),
    outputs: Annotated[str, typer.Option(help="Directory for output")] = BUILD_DIR,
    templates: Annotated[str, typer.Option(help="Directory where the template files are")] = TEMPLATE_DIR,
    days: Annotated[int, typer.Option(help="Days to spread the run over")] = 27

):
    """Generate the Python files for each dagster job"""
    
    generate_code(config, outputs, templates, days)
    generate_repository(config, outputs)
    
    # in every dir in the BUILD_DIR create an empty __init__.py
    for root, dirs, files in os.walk(BUILD_DIR):
        for name in dirs:
            if name != '__pycache__':
                os.makedirs(os.path.join(root, name), exist_ok=True)
                with open(os.path.join(root, name, "__init__.py"), 'w') as file:
                    file.write("")


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
    generate_python_jobs()

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