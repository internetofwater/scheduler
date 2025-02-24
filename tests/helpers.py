# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import requests

"""
All functions in this file are helpers for testing
and are not needed in the main pipeline
"""

GRAPH_URL_IN_TESTING = "http://localhost:7200/repositories/iow"


def clear_graph():
    """Reset the graph before running tests to make sure we are operating on fresh graph"""
    query = "CLEAR ALL"
    response = requests.post(
        f"{GRAPH_URL_IN_TESTING}/statements",
        data=query,
        headers={"Content-Type": "application/sparql-update"},
    )
    assert response.ok, response.text


def execute_sparql(query: str) -> dict[str, list[str]]:
    """Run a sparql query on graphdb and return the results"""

    # Execute the SPARQL query
    response = requests.get(
        GRAPH_URL_IN_TESTING,
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
