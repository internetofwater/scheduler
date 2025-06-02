# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from userCode.lib.types import cli_modes
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
from userCode.lib.utils import run_docker_image


class SitemapHarvestContainer:
    """A container for running web crawl operations"""

    def __init__(self, source: str) -> None:
        self.source = source

    def run(self):
        assert Path("/tmp/geoconnex/").exists(), (
            "the /tmp/geoconnex directory does not exist. This must exist for us to share configs with the docker socket on the host"
        )

        argsAsStr = (
            f"harvest "
            f"--sitemap-index sitemap.xml "
            f"--source {self.source} "
            f"--address {S3_ADDRESS} "
            f"--port {S3_PORT} "
            f"--s3-access-key {S3_ACCESS_KEY} "
            f"--s3-secret-key {S3_SECRET_KEY} "
            f"--bucket {S3_DEFAULT_BUCKET} "
            f"--log-level {GLEANER_LOG_LEVEL} "
            f"--concurrent-sitemaps {GLEANER_CONCURRENT_SITEMAPS} "
            f"--sitemap-workers {GLEANER_SITEMAP_WORKERS}"
        )

        if S3_USE_SSL:
            argsAsStr += " --ssl"

        run_docker_image(
            self.source,
            NABU_IMAGE,
            argsAsStr,
            "gleaner",
            volumeMapping=["/tmp/geoconnex/sitemap.xml:/app/sitemap.xml"],
        )


class SynchronizerContainer:
    """A container for running nabu graph sync operations"""

    def __init__(self, operation_name: cli_modes, partition: str):
        self.source = partition
        self.operation: cli_modes = operation_name

    def run(self, args: str):
        # args that should be applied to all nabu commands
        configArgs = (
            f"--upsert-batch-size {NABU_BATCH_SIZE} "
            f"--bucket {S3_DEFAULT_BUCKET} "
            f"--address {S3_ADDRESS} "
            f"--port {S3_PORT} "
            f"--s3-access-key {S3_ACCESS_KEY} "
            f"--s3-secret-key {S3_SECRET_KEY} "
            f"--log-level {NABU_LOG_LEVEL} "
            f"--endpoint {TRIPLESTORE_URL} "
        )

        argsAsStr = args + " " + configArgs

        if NABU_PROFILING:
            argsAsStr += " --trace"

        if S3_USE_SSL:
            argsAsStr += " --ssl"

        run_docker_image(
            self.source,
            NABU_IMAGE,
            argsAsStr,
            self.operation,
        )
