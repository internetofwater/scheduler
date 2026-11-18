import hashlib
import json
import os
import statistics
import sys
import time
import urllib.parse
import urllib.request

port = sys.argv[1]
label = sys.argv[2]
explorer_queries = os.path.join(
    os.path.dirname(__file__), "../../userCode/assetGroups/queries/explorer"
)
qs = {
    name.removesuffix(".sparql"): open(os.path.join(explorer_queries, name)).read()
    for name in sorted(os.listdir(explorer_queries))
    if name.endswith(".sparql")
}
base = f"http://localhost:{port}"


def post(data):
    req = urllib.request.Request(
        base,
        data=urllib.parse.urlencode(data).encode(),
        headers={"Accept": "application/sparql-results+json"},
    )
    return urllib.request.urlopen(req, timeout=600).read()


def clear():
    urllib.request.urlopen(
        base + "/?cmd=clear-cache&access-token=tok", timeout=60
    ).read()


res = {}
for ms in sys.argv[3:]:
    for n, q in qs.items():
        q = q.replace("mainstems/1>", f"mainstems/{ms}>")
        ts = []
        for _ in range(3):
            clear()
            t = time.time()
            body = post({"query": q})
            ts.append(time.time() - t)
        rows = json.loads(body)["results"]["bindings"]
        h = hashlib.md5(
            json.dumps(sorted(json.dumps(r, sort_keys=True) for r in rows)).encode()
        ).hexdigest()[:8]
        print(
            f"{label}\tms={ms}\t{n}\trows={len(rows)}\tmedian={statistics.median(ts):.3f}s\tmax={max(ts):.3f}\thash={h}",
            flush=True,
        )
