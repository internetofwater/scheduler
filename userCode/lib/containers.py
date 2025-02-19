from userCode.lib.types import cli_modes
from userCode.lib.env import GLEANER_IMAGE, NABU_IMAGE, NABU_PROFILING
from userCode.lib.utils import run_scheduler_docker_image


class GleanerContainer:
    """A container for running gleaner web crawl operations"""

    def __init__(
        self,
        source: str,
    ):
        self.name = "gleaner"
        self.source = source

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

        run_scheduler_docker_image(
            self.source,
            NABU_IMAGE,
            args,
            self.operation,
            volumeMapping=["/tmp/geoconnex/nabuconfig.yaml:/app/nabuconfig.yaml"],
        )
