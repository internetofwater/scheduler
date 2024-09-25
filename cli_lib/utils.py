import os
import subprocess

import pty
from collections import namedtuple


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
