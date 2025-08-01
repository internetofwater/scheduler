# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from dagster import Config

from userCode.lib.env import (
    GLEANER_CONCURRENT_SITEMAPS,
    GLEANER_LOG_LEVEL,
    GLEANER_SITEMAP_WORKERS,
    NABU_BATCH_SIZE,
    NABU_IMAGE,
    NABU_LOG_LEVEL,
    NABU_PROFILING,
    S3_ACCESS_KEY,
    S3_ADDRESS,
    S3_DEFAULT_BUCKET,
    S3_PORT,
    S3_SECRET_KEY,
    S3_USE_SSL,
    TRIPLESTORE_URL,
)
from userCode.lib.types import cli_modes
from userCode.lib.utils import run_docker_image


class SitemapHarvestConfig(Config):
    """
    Configuration for running web crawl operations
    This is essentially just a serialized version of our env vars;
    and it uses the env vars as default
    """

    address: str = S3_ADDRESS
    port: str = S3_PORT
    s3_access_key: str = S3_ACCESS_KEY
    s3_secret_key: str = S3_SECRET_KEY
    bucket: str = S3_DEFAULT_BUCKET
    log_level: str = GLEANER_LOG_LEVEL
    concurrent_sitemaps: int = GLEANER_CONCURRENT_SITEMAPS
    sitemap_workers: int = GLEANER_SITEMAP_WORKERS
    validate_shacl: bool = False
    useSSL: bool = S3_USE_SSL

    # whether or not to raise an exception upon encountering a 3 exit code
    exit_3_is_fatal: bool = False


class SitemapHarvestContainer:
    """A container for running web crawl operations"""

    def __init__(self, source: str) -> None:
        self.source = source

    def run(self, config: SitemapHarvestConfig):
        assert Path("/tmp/geoconnex/").exists(), (
            "the /tmp/geoconnex directory does not exist. This must exist for us to share configs with the docker socket on the host"
        )

        argsAsStr = (
            f"harvest "
            f"--sitemap-index sitemap.xml "
            f"--source {self.source} "
            f"--address {config.address} "
            f"--port {config.port} "
            f"--s3-access-key {config.s3_access_key} "
            f"--s3-secret-key {config.s3_secret_key} "
            f"--bucket {config.bucket} "
            f"--log-level {config.log_level} "
            f"--concurrent-sitemaps {config.concurrent_sitemaps} "
            f"--sitemap-workers {config.sitemap_workers} "
            f"--log-as-json "
        )

        if config.useSSL:
            argsAsStr += " --ssl "

        if config.validate_shacl:
            argsAsStr += " --validate-shacl "

        run_docker_image(
            self.source,
            NABU_IMAGE,
            argsAsStr,
            "sitemap_harvest",
            exit_3_is_fatal=config.exit_3_is_fatal,
            volumeMapping=["/tmp/geoconnex/sitemap.xml:/app/sitemap.xml"],
        )


class SynchronizerConfig(Config):
    """
    Configuration for running nabu graph sync operations
    This is essentially just a serialized version of our env vars
    """

    upsertBatchSize: int = NABU_BATCH_SIZE
    bucket: str = S3_DEFAULT_BUCKET
    address: str = S3_ADDRESS
    port: str = S3_PORT
    s3_access_key: str = S3_ACCESS_KEY
    s3_secret_key: str = S3_SECRET_KEY
    log_level: str = NABU_LOG_LEVEL
    endpoint: str = TRIPLESTORE_URL
    useSSL: bool = S3_USE_SSL
    profiling: bool = NABU_PROFILING


class SynchronizerContainer:
    """A container for running nabu graph sync operations"""

    def __init__(
        self,
        operation_name: cli_modes,
        partition: str,
        volume_mapping: list[str] | None = None,
    ):
        self.source = partition
        self.operation: cli_modes = operation_name
        self.volume_mapping = volume_mapping

    def run(self, args: str, config: SynchronizerConfig):
        # args that should be applied to all nabu commands
        configArgs = (
            f"--upsert-batch-size {config.upsertBatchSize} "
            f"--bucket {config.bucket} "
            f"--address {config.address} "
            f"--port {config.port} "
            f"--s3-access-key {config.s3_access_key} "
            f"--s3-secret-key {config.s3_secret_key} "
            f"--log-level {config.log_level} "
            f"--endpoint {config.endpoint} "
            f"--log-as-json "
        )

        argsAsStr = args + " " + configArgs

        if config.useSSL:
            argsAsStr += " --ssl"

        if config.profiling:
            argsAsStr += " --trace"

        run_docker_image(
            self.source,
            NABU_IMAGE,
            argsAsStr,
            self.operation,
            volumeMapping=self.volume_mapping,
        )
