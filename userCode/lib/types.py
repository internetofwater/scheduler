# Copyright 2025 Company, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Literal, TypeAlias, TypedDict


cli_modes: TypeAlias = Literal[
    # All the options for either gleaner or nabu
    "gleaner",
    # All the cli modes that nabu can run
    "release",
    "object",
    "prune",
    "prov-release",
    "prov-clear",
    "prov-object",
    "prov-drain",
    "orgs-release",
    "orgs",
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
