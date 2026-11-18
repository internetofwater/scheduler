import hashlib
import json
import os
import statistics
import sys
import time
import urllib.parse
import urllib.request

port = sys.argv[1]
base = f"http://localhost:{port}"
body = (
    open(
        os.path.join(
            os.path.dirname(__file__),
            "../../userCode/assetGroups/queries/explorer_datasets_materialized_view.sparql",
        )
    )
    .read()
    .replace("mainstems/1>", "mainstems/MS>")
)
inner = body
datasets = body
hdr, rest = body.split("SELECT DISTINCT", 1)
count = (
    hdr
    + "SELECT (COUNT(*) AS ?count) WHERE { { SELECT DISTINCT"
    + rest.rstrip()
    + " } }"
)


def post(q):
    req = urllib.request.Request(
        base,
        data=urllib.parse.urlencode({"query": q}).encode(),
        headers={"Accept": "application/sparql-results+json"},
    )
    return urllib.request.urlopen(req, timeout=600).read()


for ms in sys.argv[2:]:
    for n, q in (("getDatasets", datasets), ("getDatasetCount", count)):
        q = q.replace("MS", ms) if "MS" in q else q
        ts = []
        for _ in range(3):
            urllib.request.urlopen(base + "/?cmd=clear-cache&access-token=tok").read()
            t = time.time()
            b = post(q)
            ts.append(time.time() - t)
        rows = json.loads(b)["results"]["bindings"]
        h = hashlib.md5(
            json.dumps(sorted(json.dumps(r, sort_keys=True) for r in rows)).encode()
        ).hexdigest()[:8]
        print(
            f"explicit\tms={ms}\t{n}\trows={len(rows)}\tmedian={statistics.median(ts):.3f}s\thash={h}",
            flush=True,
        )
