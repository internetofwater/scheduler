import requests


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
