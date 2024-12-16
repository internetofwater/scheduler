import io
from pathlib import Path
import subprocess
import sys
from typing import Any
from dagster import get_dagster_logger
from minio import Minio
from urllib3 import BaseHTTPResponse

from .env import (
    GLEANER_MINIO_SECRET_KEY,
    GLEANER_MINIO_ACCESS_KEY,
    GLEANER_MINIO_BUCKET,
    GLEANER_MINIO_ADDRESS,
    GLEANER_MINIO_PORT,
    GLEANER_MINIO_USE_SSL,
    RCLONE_ENDPOINT_URL,
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


class RClone:
    @classmethod
    def get_config_path(cls) -> Path:
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
        rclone_conf_location = self.get_config_path()
        with open(str(rclone_conf_location), "w") as f:
            f.write(config_data)

    def _run_subprocess(self, command: str):
        """Run a shell command and stream the output in realtime"""
        process = subprocess.Popen(
            command,
            shell=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            sys.exit(
                f"{command} failed with exit code {process.returncode} {stderr=} {stdout=}"
            )

        return process.returncode, stdout, stderr

    def copy(self, path_to_file: str):
        get_dagster_logger().info(f"Uploading {path_to_file} to {RCLONE_ENDPOINT_URL}")
        returncode = self._run_subprocess(
            f"rclone copy minio:{GLEANER_MINIO_BUCKET}/{path_to_file} lakefs:geoconnex/main/{path_to_file}"
        )

        if returncode != 0:
            raise Exception(f"Error copying {path_to_file} to {RCLONE_ENDPOINT_URL}.")
