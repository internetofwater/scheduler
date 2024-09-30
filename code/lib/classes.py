import io
from typing import Any
from dagster import get_dagster_logger
from minio import Minio
from urllib3 import HTTPResponse
from .env import (
    GLEANER_MINIO_SECRET_KEY,
    GLEANER_MINIO_ACCESS_KEY,
    GLEANER_MINIO_BUCKET,
    GLEANER_MINIO_ADDRESS,
    GLEANER_MINIO_PORT,
    GLEANER_MINIO_USE_SSL,
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
        logger.debug(f"S3 object path : {remote_path}")
        response: HTTPResponse = self.client.get_object(
            GLEANER_MINIO_BUCKET, remote_path
        )
        data = response.read()
        response.close()
        response.release_conn()
        return data
