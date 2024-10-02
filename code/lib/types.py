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
