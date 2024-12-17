import io
from pathlib import Path
import subprocess
import sys
from typing import Any
from dagster import get_dagster_logger
from minio import Minio
from urllib3 import BaseHTTPResponse
from lakefs.client import Client

from userCode.lib.lakefsUtils import create_branch_if_not_exists
from .env import (
    GLEANER_MINIO_SECRET_KEY,
    GLEANER_MINIO_ACCESS_KEY,
    GLEANER_MINIO_BUCKET,
    GLEANER_MINIO_ADDRESS,
    GLEANER_MINIO_PORT,
    GLEANER_MINIO_USE_SSL,
    LAKEFS_ACCESS_KEY_ID,
    LAKEFS_ENDPOINT_URL,
    LAKEFS_SECRET_ACCESS_KEY,
)


class S3:
    def __init__(self):
        self.endpoint = f"{GLEANER_MINIO_ADDRESS}:{GLEANER_MINIO_PORT}"
        self.client = Minio(
            self.endpoint,
            secure=GLEANER_MINIO_USE_SSL,
            access_key=GLEANER_MINIO_ACCESS_KEY,
            secret_key=GLEANER_MINIO_SECRET_KEY,
        )

    def load(self, data: Any, remote_path: str):
        """Load arbitrary data into s3 bucket"""
        f = io.BytesIO()
        length = f.write(data)
        f.seek(0)  # Reset the stream position to the beginning for reading
        self.client.put_object(
            GLEANER_MINIO_BUCKET,
            remote_path,
            f,
            length,
            content_type="text/plain",
        )
        get_dagster_logger().info(f"Uploaded '{remote_path.split('/')[-1]}'")

    def read(self, remote_path: str):
        logger = get_dagster_logger()
        logger.info(f"S3 URL    : {GLEANER_MINIO_ADDRESS}")
        logger.info(f"S3 SERVER : {self.endpoint}")
        logger.info(f"S3 PORT   : {GLEANER_MINIO_PORT}")
        logger.info(f"S3 BUCKET : {GLEANER_MINIO_BUCKET}")
        logger.debug(f"S3 object path : {remote_path}∂")
        response: BaseHTTPResponse = self.client.get_object(
            GLEANER_MINIO_BUCKET, remote_path
        )
        data = response.read()
        response.close()
        response.release_conn()
        return data


class FileTransferer:
    """Helper class to transfer files from minio to lakefs using rclone"""

    @classmethod
    def get_rclone_config_path(cls) -> Path:
        """
        Get the path to the rclone clone config file on the host.
        This is needed since rclone configs can be present in multiple locations
        """
        # Run the command and capture its output
        result = subprocess.run(
            ["rclone", "config", "file"],
            text=True,  # Ensure output is returned as a string
            stdout=subprocess.PIPE,  # Capture standard output
            stderr=subprocess.PIPE,  # Capture standard error
        )
        if result.returncode == 0:
            # Parse the output to get the path
            for line in result.stdout.splitlines():
                if line.startswith("/"):  # Configuration paths typically start with '/'
                    return Path(line.strip())

        raise RuntimeError("Error finding rclone config file path:", result.stderr)

    def __init__(self, config_data: str):
        rclone_conf_location = self.get_rclone_config_path()
        with open(str(rclone_conf_location), "w") as f:
            f.write(config_data)

        self.lakefs_client = Client(
            host=LAKEFS_ENDPOINT_URL,
            username=LAKEFS_ACCESS_KEY_ID,
            password=LAKEFS_SECRET_ACCESS_KEY,
        )

    def _run_subprocess(self, command: str):
        """Run a shell command and stream the output in realtime"""

        process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
        )
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            sys.exit(
                f"Error, returned code: {process.returncode} with output: {stderr}"
            )

        return stdout, stderr

    def copy_to_lakefs(self, path_to_file: str):
        """
        Copy a file from minio to lakefs

        path_to_file must be a path relative to the minio bucket
        i.e. copy(test_dir/test_file.json) will copy from `gleanerbucket/test_dir/test_file.json`
        """

        get_dagster_logger().info(f"Uploading {path_to_file} to {LAKEFS_ENDPOINT_URL}")

        branch_name = "develop"  # name of the branch where we want to put new files before adding them into main

        new_branch = create_branch_if_not_exists(branch_name)

        self._run_subprocess(
            f"rclone copy minio:{GLEANER_MINIO_BUCKET}/{path_to_file} lakefs:geoconnex/{branch_name} -v"
        )

        if list(new_branch.uncommitted()):
            new_branch.commit(
                message=f"Adding {path_to_file} automatically from the geoconnex scheduler"
            )
            result = new_branch.merge_into("main")
            get_dagster_logger().info(result)
        else:
            get_dagster_logger().warning(
                """"The lakefs client copied a file but no new changes were detected on the remote lakefs cluster. 
                This is a sign that either the file was already present or something may be wrong"""
            )
