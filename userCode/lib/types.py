# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from typing import Literal, TypeAlias

cli_modes: TypeAlias = Literal[
    # All the options for either gleaner or nabu
    "sitemap_harvest",
    # All the cli modes that nabu can run
    "release",
    "object",
    "prov-release",
    "prov-clear",
    "orgs-release",
    "orgs",
    "pull",
]
