
import re
import os 
import subprocess
from jinja2 import Environment, FileSystemLoader

import pty
from collections import namedtuple

def remove_non_alphanumeric(string):
    return re.sub(r'[^a-zA-Z0-9_]+', '', string)

def strict_env(env_var: str) -> str:
    val = os.environ.get(env_var)
    if val is None:
        raise RuntimeError(f"Missing required environment variable: {env_var}")
    return val


CommandResult = namedtuple('CommandResult', ['stdout', 'stderr', 'returncode'])

def run_command(command: str, print_output: bool = True) -> CommandResult:
    """Given a command string, run it and display the results in the console in realtime"""
    master_fd, slave_fd = pty.openpty()
    
    process = subprocess.Popen(
        command,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=slave_fd,
        stderr=subprocess.PIPE,
        close_fds=True,
        bufsize=1,
        universal_newlines=True,
    )
    
    os.close(slave_fd)
    
    stdout = []
    stderr = []
    
    def read_fd(fd, output_list):
        while True:
            data = os.read(fd, 1024)
            if not data:
                break
            output_list.append(data.decode())
            if print_output:
                print(data.decode(), end='')

    stdout_reader = os.fdopen(master_fd)
    
    while process.poll() is None:
        read_fd(master_fd, stdout)
    
    # Read any remaining output after the process ends
    read_fd(master_fd, stdout)
    
    # Close the file descriptor
    stdout_reader.close()
    
    _, stderr_output = process.communicate()
    stderr.append(stderr_output)
    
    return CommandResult(''.join(stdout), ''.join(stderr), process.returncode)


def template_config(base, out_dir):
    env = Environment(loader=FileSystemLoader(os.path.dirname(base)))
    template = env.get_template(os.path.basename(base)) 

    # Render the template with the context
    rendered_content = template.render(**get_common_env())

    # Write the rendered content to the output file
    output_name = str(os.path.basename(base)).removesuffix('.j2')

    with open(os.path.join(out_dir, output_name), 'w+') as file:
        file.write(rendered_content)

    return os.path.join(out_dir, output_name)


def get_common_env():
    """All env files here are used in templating configs since they are used in both gleaner and nabu configs"""
    return {
        "GLEANERIO_MINIO_ADDRESS": strict_env("GLEANERIO_MINIO_ADDRESS"),
        "MINIO_ACCESS_KEY": strict_env("MINIO_ACCESS_KEY"),
        "MINIO_SECRET_KEY": strict_env("MINIO_SECRET_KEY"),
        "GLEANERIO_MINIO_BUCKET": strict_env("GLEANERIO_MINIO_BUCKET"),
        "GLEANERIO_MINIO_PORT": strict_env("GLEANERIO_MINIO_PORT"),
        "GLEANERIO_MINIO_USE_SSL": strict_env("GLEANERIO_MINIO_USE_SSL"),
    }