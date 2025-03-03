# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import io
from pathlib import Path
import subprocess
import sys
from typing import Any
from dagster import get_dagster_logger
from minio import Minio
from urllib3 import BaseHTTPResponse, PoolManager, Retry
from lakefs.client import Client

from userCode.lib.lakefs import LakeFSClient

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
    RUNNING_AS_TEST_OR_DEV,
)


class S3:
    def __init__(self):
        # If we are in a test environment then we want to use localhost
        # since we are outside of the docker network.
        if RUNNING_AS_TEST_OR_DEV() and "googleapis.com" not in GLEANER_MINIO_ADDRESS:
            self.endpoint = f"localhost:{GLEANER_MINIO_PORT}"
        else:
            self.endpoint = f"{GLEANER_MINIO_ADDRESS}:{GLEANER_MINIO_PORT}"
        self.client = Minio(
            self.endpoint,
            secure=GLEANER_MINIO_USE_SSL,
            access_key=GLEANER_MINIO_ACCESS_KEY,
            secret_key=GLEANER_MINIO_SECRET_KEY,
            http_client=PoolManager(
                num_pools=40,
                maxsize=40,
                retries=Retry(
                    total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504]
                ),
            ),
        )

        if not self.client.bucket_exists(GLEANER_MINIO_BUCKET):
            self.client.make_bucket(GLEANER_MINIO_BUCKET)

    def object_has_content(self, remote_path: str) -> bool:
        obj = self.client.stat_object(GLEANER_MINIO_BUCKET, remote_path)
        return any(
            [
                obj is not None and obj.size is not None and obj.size != 0,
                obj is not None
                and obj.metadata is not None
                and float(obj.metadata.get("x-goog-stored-content-length", 0)) > 0,
            ]
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

    def load_stream(
        self,
        stream,
        remote_path: str,
        content_length: int,
        content_type: str,
        headers={},
    ):
        """Stream data into S3 without loading it all into memory"""
        self.client.put_object(
            GLEANER_MINIO_BUCKET,
            remote_path,
            stream,
            content_length,
            content_type=content_type,
            part_size=8 * 1024 * 1024,
            metadata=headers,
        )
        get_dagster_logger().info(
            f"Uploaded '{remote_path.split('/')[-1]}' via streaming"
        )

    def read(self, remote_path: str) -> bytes:
        """Read an object from S3 and return it as bytes"""
        logger = get_dagster_logger()
        logger.info(f"S3 endpoint that dagster will connect to: {self.endpoint}")
        logger.info(f"S3 Address that gleaner will use: {GLEANER_MINIO_ADDRESS}")
        logger.info(f"S3 BUCKET : {GLEANER_MINIO_BUCKET}")
        logger.debug(f"S3 object path : {remote_path}")
        response: BaseHTTPResponse = self.client.get_object(
            GLEANER_MINIO_BUCKET, remote_path
        )
        data = response.read()
        response.close()
        response.release_conn()
        return data

    def read_stream(self, remote_path: str):
        """Read an object from S3 as a stream"""
        response: BaseHTTPResponse = self.client.get_object(
            GLEANER_MINIO_BUCKET, remote_path
        )
        try:
            for chunk in response.stream(8 * 1024 * 1024):
                yield chunk
        finally:
            response.close()
            response.release_conn()


class RcloneClient:
    """Helper class to transfer files from minio to lakefs using rclone"""

    @staticmethod
    def rclone_client():
        return "rclone" if RUNNING_AS_TEST_OR_DEV() else "/root/.local/bin/rclone"

    @classmethod
    def get_config_path(cls) -> Path:
        """
        Get the path to the rclone clone config file on the host.
        This is needed since rclone configs can be present in multiple locations
        """
        # Run the command and capture its output
        result = subprocess.run(
            [cls.rclone_client(), "config", "file"],
            text=True,  # Ensure output is returned as a string
            stdout=subprocess.PIPE,  # Capture standard output
            stderr=subprocess.PIPE,  # Capture standard error
            check=True,
        )
        if result.returncode == 0:
            # Parse the output to get the path
            for line in result.stdout.splitlines():
                if line.startswith("/"):  # Configuration paths typically start with '/'
                    return Path(line.strip())

        raise RuntimeError("Error finding rclone config file path:", result.stderr)

    def __init__(self, config_data: str):
        rclone_conf_location = self.get_config_path()
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

    def copy_to_lakefs(
        self,
        path_to_file: str,
        destination_filename: str,
        destination_branch: str,
        lakefs_client: LakeFSClient,
    ):
        """
        Copy a file from minio to lakefs

        path_to_file must be a path relative to the bucket name within minio
        i.e. copy(test_dir/test_file.json) will copy from `gleanerbucket/test_dir/test_file.json`

        destination_branch is the name of the lakefs branch to copy the file to
        """

        get_dagster_logger().info(f"Uploading {path_to_file} to {LAKEFS_ENDPOINT_URL}")

        new_branch = lakefs_client.create_branch_if_not_exists(destination_branch)

        cmd_to_run = f"{self.rclone_client()} copyto s3:{GLEANER_MINIO_BUCKET}/{path_to_file} lakefs:geoconnex/{destination_branch}/{destination_filename} -v --s3-upload-concurrency 8"
        get_dagster_logger().info(f"Running bash command: {cmd_to_run}")
        self._run_subprocess(cmd_to_run)

        if list(new_branch.uncommitted()):
            get_dagster_logger().info(f"Commiting file to: {destination_branch}")
            new_branch.commit(
                message=f"Adding {path_to_file} automatically from the geoconnex scheduler",
                metadata={},
            )
        else:
            get_dagster_logger().warning(
                """"The lakefs client copied a file but no new changes were detected on the remote lakefs cluster. 
                This is a sign that either the file was already present or something may be wrong"""
            )
