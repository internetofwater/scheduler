
from dataclasses import dataclass
from typing import Literal, TypeAlias, TypedDict


cli_modes: TypeAlias = Literal[
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

@dataclass
class S3ObjectInfo:
    """Metadata for uploading into S3"""
    bucket_name: str
    object_name: str


class GleanerSource(TypedDict):
    active: str
    domain: str 
    headless: str 
    name: str 
    pid: str 
    propername: str
    sourcetype: str 
    url: str