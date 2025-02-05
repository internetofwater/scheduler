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

import requests


def clear_graph():
    """Reset the graph before running tests to make sure we are operating on fresh graph"""
    query = "CLEAR ALL"
    response = requests.post(
        "http://localhost:7200/repositories/iow/statements",
        data=query,
        headers={"Content-Type": "application/sparql-update"},
    )
    assert response.ok, response.text


def execute_sparql(query: str) -> dict[str, list[str]]:
    """Run a sparql query on graphdb and return the results"""
    endpoint = "http://localhost:7200/repositories/iow"

    # Execute the SPARQL query
    response = requests.get(
        endpoint,
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
