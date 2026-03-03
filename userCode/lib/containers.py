# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


from dagster import Config

from userCode.lib.env import (
    GLEANER_CONCURRENT_SITEMAPS,
    GLEANER_LOG_LEVEL,
    GLEANER_SHACL_VALIDATOR_GRPC_ENDPOINT,
    GLEANER_SITEMAP_INDEX,
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
)
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
    metadata_bucket: str = S3_METADATA_BUCKET
    log_level: str = GLEANER_LOG_LEVEL
    concurrent_sitemaps: int = GLEANER_CONCURRENT_SITEMAPS
    sitemap_workers: int = GLEANER_SITEMAP_WORKERS
    shacl_validation_grpc_endpoint: str = GLEANER_SHACL_VALIDATOR_GRPC_ENDPOINT
    useShacl: bool = GLEANER_USE_SHACL
    useSSL: bool = S3_USE_SSL
    # make a shacl validation error fail the pipeline
    exit_on_shacl_failure: bool = False
    # whether or not to raise an exception upon encountering a 3 exit code
    exit_3_is_fatal: bool = False
    # Remove any jsonld that we didn't find during the crawl
    cleanup_outdated_jsonld: bool = True


class SitemapHarvestContainer:
    """A container for running web crawl operations"""

    def __init__(self, source: str) -> None:
        self.source = source

    def run(self, config: SitemapHarvestConfig):
        argsAsStr = (
            f"harvest "
            f"--sitemap-index {GLEANER_SITEMAP_INDEX} "
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
            f"--log-as-json "
        )

        if config.useSSL:
            argsAsStr += " --ssl "

        if config.useShacl:
            argsAsStr += (
                " --shacl-grpc-endpoint " + config.shacl_validation_grpc_endpoint
            )

        if config.exit_on_shacl_failure:
            argsAsStr += " --exit-on-shacl-failure "

        if config.cleanup_outdated_jsonld:
            argsAsStr += " --cleanup-outdated-jsonld "

        run_docker_image(
            self.source,
            NABU_IMAGE,
            argsAsStr,
            exit_3_is_fatal=config.exit_3_is_fatal,
            # the docker sock must be mounted for bulk sitemap operations
            # this allows nabu to spin up containers in the sitemap.xml file
            volumeMapping=["/var/run/docker.sock:/var/run/docker.sock"],
        )


class NqConfig(Config):
    """
    Configuration for running nabu release / pull graph operations
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


class NqOperationsContainer:
    """A container for running nabu release graph generation operations"""

    def __init__(
        self,
        partition: str | None,
        volume_mapping: list[str] | None = None,
        mainstem_file: None | str = None,
    ):
        self.partition = partition if partition else "no_partition_specified"
        self.volume_mapping = volume_mapping
        self.mainstem_file = mainstem_file

    def run(self, args: str, config: NqConfig):
        # args that should be applied to all nabu commands
        configArgs = (
            f"--bucket {config.bucket} "
            f"--address {config.address} "
            f"--port {config.port} "
            f"--s3-access-key {config.s3_access_key} "
            f"--s3-secret-key {config.s3_secret_key} "
            f"--log-level {config.log_level} "
            f"--log-as-json "
        )

        argsAsStr = args + " " + configArgs

        if config.useSSL:
            argsAsStr += " --ssl"

        if config.profiling:
            argsAsStr += " --trace"

        # only add mainstem info to release nquads; other operations on provenance data
        # or orgs has no geospatial data and thus checking for mainstem would be pointless
        if self.mainstem_file:
            # we can hard code the path since it is mounted with a volume
            # and thus will always be the same
            argsAsStr += f" --mainstem-metadata {self.mainstem_file} "

        run_docker_image(
            self.partition,
            NABU_IMAGE,
            argsAsStr,
            volumeMapping=self.volume_mapping,
        )
