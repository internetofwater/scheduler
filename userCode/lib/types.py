# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from typing import Literal, TypeAlias, TypedDict

cli_modes: TypeAlias = Literal[
    # All the options for either gleaner or nabu
    "sitemap_harvest",
    # All the cli modes that nabu can run
    "release",
    "object",
    "sync",
    "prov-release",
    "prov-clear",
    "prov-object",
    "orgs-release",
    "orgs",
    "pull",
]


class GleanerSource(TypedDict):
    """Represents one 'source' block in the gleaner yaml config file"""

    active: str
    domain: str

    # this is a string of "true" or "false"
    headless: str

    name: str
    pid: str
    propername: str
    sourcetype: str
    url: str


class GleanerConfig(TypedDict):
    """Represents the entire gleaner yaml config file"""

    sources: list[GleanerSource]
    context: dict
    gleaner: dict
    millers: dict
    minio: dict
