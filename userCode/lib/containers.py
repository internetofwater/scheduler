# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path
from userCode.lib.types import cli_modes
from userCode.lib.env import GLEANER_IMAGE, NABU_BATCH_SIZE, NABU_IMAGE, NABU_PROFILING
from userCode.lib.utils import run_scheduler_docker_image


class GleanerContainer:
    """A container for running gleaner web crawl operations"""

    def __init__(
        self,
        source: str,
    ):
        self.name = "gleaner"
        self.source = source

        assert Path(
            "/tmp/geoconnex/"
        ).exists(), "the /tmp/geoconnex directory does not exist. This must exist for us to share configs with the docker socket on the host"

    def run(self, args: list[str]):
        run_scheduler_docker_image(
            self.source,
            GLEANER_IMAGE,
            args,
            "gleaner",
            volumeMapping=["/tmp/geoconnex/gleanerconfig.yaml:/app/gleanerconfig.yaml"],
        )


class NabuContainer:
    """A container for running nabu graph sync operations"""

    def __init__(self, operation_name: cli_modes, partition: str):
        self.source = partition
        self.operation: cli_modes = operation_name

    def run(self, args: list[str]):
        if NABU_PROFILING:
            args.append("--trace")

        args.append(f"--upsert-batch-size={NABU_BATCH_SIZE}")

        nabu_log_level = os.environ.get("NABU_LOG_LEVEL")
        if nabu_log_level:
            args.append(f"--log-level={nabu_log_level}")

        run_scheduler_docker_image(
            self.source,
            NABU_IMAGE,
            args,
            self.operation,
            volumeMapping=["/tmp/geoconnex/nabuconfig.yaml:/app/nabuconfig.yaml"],
        )
