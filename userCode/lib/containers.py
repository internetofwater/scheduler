# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


from pathlib import Path

from dagster import Config

from userCode.lib.env import (
    GLEANER_CONCURRENT_SITEMAPS,
    GLEANER_LOG_LEVEL,
    GLEANER_SITEMAP_WORKERS,
    GLEANER_USE_SHACL,
    NABU_IMAGE,
    NABU_LOG_LEVEL,
    NABU_PROFILING,
    S3_ACCESS_KEY,
    S3_ADDRESS,
    S3_DEFAULT_BUCKET,
    S3_METADATA_BUCKET,
    S3_PORT,
    S3_SECRET_KEY,
    S3_USE_SSL,
    SITEMAP_INDEX,
)
from userCode.lib.utils import run_docker_image, run_docker_image_to_gzip_file


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
    metadata_bucket: str = S3_METADATA_BUCKET
    log_level: str = GLEANER_LOG_LEVEL
    concurrent_sitemaps: int = GLEANER_CONCURRENT_SITEMAPS
    sitemap_workers: int = GLEANER_SITEMAP_WORKERS
    useShacl: bool = GLEANER_USE_SHACL
    useSSL: bool = S3_USE_SSL
    # make a shacl validation error fail the pipeline
    exit_on_shacl_failure: bool = False
    # whether or not to raise an exception upon encountering a 3 exit code
    exit_3_is_fatal: bool = False


class SitemapHarvestContainer:
    """A container for running web crawl operations"""

    def __init__(
        self,
        source: str,
        mainstem_file: str,
        volume_mapping: list[str] | None = None,
    ) -> None:
        self.source = source
        # the mainstem file is used by nabu to add the associated mainstem
        # to the jsonld of sitemaps which request it in the sitemap index
        self.mainstem_file = mainstem_file
        self.volume_mapping = volume_mapping or []

    def run(self, config: SitemapHarvestConfig):
        argsAsStr = (
            f"harvest "
            f"--sitemap-index {SITEMAP_INDEX} "
            f"--source {self.source} "
            f"--address {config.address} "
            f"--port {config.port} "
            f"--s3-access-key {config.s3_access_key} "
            f"--s3-secret-key {config.s3_secret_key} "
            f"--bucket {config.bucket} "
            f"--metadata-bucket {config.metadata_bucket} "
            f"--log-level {config.log_level} "
            f"--concurrent-sitemaps {config.concurrent_sitemaps} "
            f"--sitemap-workers {config.sitemap_workers} "
            f"--mainstem-metadata {self.mainstem_file} "
            f"--log-as-json "
        )

        if config.useSSL:
            argsAsStr += " --ssl "

        if config.useShacl:
            argsAsStr += " --shacl-local "

        if config.exit_on_shacl_failure:
            argsAsStr += " --exit-on-shacl-failure "

        run_docker_image(
            self.source,
            NABU_IMAGE,
            argsAsStr,
            exit_3_is_fatal=config.exit_3_is_fatal,
            # the docker sock must be mounted for bulk sitemap operations
            # this allows nabu to spin up containers in the sitemap.xml file
            volumeMapping=["/var/run/docker.sock:/var/run/docker.sock"]
            + self.volume_mapping,
        )


class NquadsConfig(Config):
    """
    Configuration for running nabu nquads operations
    This is essentially just a serialized version of our env vars
    """

    bucket: str = S3_DEFAULT_BUCKET
    address: str = S3_ADDRESS
    port: str = S3_PORT
    s3_access_key: str = S3_ACCESS_KEY
    s3_secret_key: str = S3_SECRET_KEY
    log_level: str = NABU_LOG_LEVEL
    useSSL: bool = S3_USE_SSL
    profiling: bool = NABU_PROFILING


class NquadsContainer:
    """
    A container for converting a harvested parquet file in s3 to nquads.
    Nquads are not stored in s3 and are instead generated from the parquet on the fly
    """

    def __init__(self, sitemap_id: str):
        self.sitemap_id = sitemap_id

    def run(self, output_file: Path, config: NquadsConfig):
        """Write the gzipped nquads of the sitemap to output_file"""
        argsAsStr = (
            f"nquads "
            f"--prefix summoned/{self.sitemap_id}.parquet "
            f"--bucket {config.bucket} "
            f"--address {config.address} "
            f"--port {config.port} "
            f"--s3-access-key {config.s3_access_key} "
            f"--s3-secret-key {config.s3_secret_key} "
            f"--log-level {config.log_level} "
            f"--log-as-json "
        )

        if config.useSSL:
            argsAsStr += " --ssl"

        if config.profiling:
            argsAsStr += " --trace"

        run_docker_image_to_gzip_file(
            self.sitemap_id,
            NABU_IMAGE,
            argsAsStr,
            output_file,
        )
