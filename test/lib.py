# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import os
from typing import Literal
import requests

from userCode.lib.classes import RcloneClient

"""
All functions in this file are helpers for testing
and are not needed in the main pipeline
"""


class SparqlClient:
    def __init__(self, repository: Literal["iow", "iowprov"] = "iow"):
        GRAPH_URL_IN_TESTING = "http://localhost:7200/repositories/"
        self.url = GRAPH_URL_IN_TESTING + repository

    def clear_graph(self):
        """Reset the graph before running tests to make sure we are operating on fresh graph"""
        query = "CLEAR ALL"
        response = requests.post(
            f"{self.url}/statements",
            data=query,
            headers={"Content-Type": "application/sparql-update"},
        )
        assert response.ok, response.text

    def insert_triples_as_graph(self, graph_name: str, triples: str):
        """Insert a named graph into the triplestore. Useful for testing"""
        sparql_update = f"""
            INSERT DATA {{
                GRAPH <{graph_name}> {{
                    {triples}
                }}
            }}
            """
        # we have to post against the statements url with the proper header
        # since it is an update and not a general stateless query
        response = requests.post(
            f"{self.url}/statements",
            data=sparql_update,
            headers={"Content-Type": "application/sparql-update"},
        )
        assert response.ok, response.text

    def execute_sparql(self, query: str) -> dict[str, list[str]]:
        """Run a sparql query on graphdb and return the results"""

        # Execute the SPARQL query
        response = requests.get(
            self.url,
            params={"query": query},
            headers={"Accept": "application/sparql-results+json"},
        )
        assert response.ok, response.text

        result = response.json()

        # the bindings are essentially the names with the corresponding values
        bindings = result["results"]["bindings"]

        # the variable names that we requested are in the head
        vars = result["head"]["vars"]

        mapping: dict[str, list[str]] = dict.fromkeys(vars, [])

        for binding in bindings:
            for var in vars:
                if var in binding:
                    mapping[var].append(binding[var]["value"])
                else:
                    raise ValueError(f"Variable {var} not found in the result")

        return mapping


def assert_rclone_is_installed_properly():
    location = RcloneClient.get_config_path()
    assert location.parent.exists(), f"{location} does not exist"
    assert os.system("rclone version") == 0
