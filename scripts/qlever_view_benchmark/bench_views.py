"""
Run every upstream explorer query (queries/explorer) and its rewrite that reads the
materialized views (queries/explorer_views) against a running qlever server, check
that both return the same rows, and print the median time of each.

Usage: python3 bench_views.py PORT ACCESS_TOKEN MAINSTEM_ID [MAINSTEM_ID ...]
"""

import hashlib
import json
from pathlib import Path
import statistics
import sys
import time
import urllib.parse
import urllib.request

QUERIES = Path(__file__).parents[2] / "userCode" / "assetGroups" / "queries"
MAINSTEM = "<https://geoconnex.us/ref/mainstems/1>"
RUNS = 3

port, token, *mainstems = sys.argv[1:]
base = f"http://localhost:{port}"


def run(query: str) -> tuple[float, list[dict]]:
    """Median wall clock over RUNS runs with a cold cache, and the result rows"""
    times = []
    bodies = []
    for _ in range(RUNS):
        urllib.request.urlopen(f"{base}/?cmd=clear-cache&access-token={token}").read()
        request = urllib.request.Request(
            base,
            data=urllib.parse.urlencode({"query": query}).encode(),
            headers={"Accept": "application/sparql-results+json"},
        )
        start = time.time()
        bodies.append(urllib.request.urlopen(request, timeout=600).read())
        times.append(time.time() - start)
    return statistics.median(times), json.loads(bodies[-1])["results"]["bindings"]


def fingerprint(rows: list[dict]) -> str:
    """Order-insensitive hash of the rows, comparing values but not datatypes"""
    values = sorted(
        json.dumps({k: v["value"] for k, v in row.items()}, sort_keys=True)
        for row in rows
    )
    return hashlib.md5(json.dumps(values).encode()).hexdigest()[:8]


mismatches = 0
print("query\tmainstem\trows\tupstream\tview\tsame")
for mainstem in mainstems:
    target = f"<https://geoconnex.us/ref/mainstems/{mainstem}>"
    for upstream in sorted((QUERIES / "explorer").glob("*.sparql")):
        rewritten = QUERIES / "explorer_views" / upstream.name
        up_time, up_rows = run(upstream.read_text().replace(MAINSTEM, target))
        view_time, view_rows = run(rewritten.read_text().replace(MAINSTEM, target))
        same = fingerprint(up_rows) == fingerprint(view_rows)
        mismatches += not same
        print(
            f"{upstream.stem}\t{mainstem}\t{len(up_rows)}\t{up_time:.3f}s\t{view_time:.3f}s\t{same}",
            flush=True,
        )
sys.exit(1 if mismatches else 0)
