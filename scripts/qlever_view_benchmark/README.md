# QLever materialized view benchmark

Compares each explorer query (`userCode/assetGroups/queries/explorer/*.sparql`, pulled from upstream)
with its rewrite that reads the materialized views in `assets/materialized_views.json`
(`userCode/assetGroups/queries/explorer_views/*.sparql`, same file name), on a synthetic graph with the
same shape as geoconnex monitoring locations (500k locations, ~14M triples, skewed so `mainstems/1` is
large, with a few locations missing a type, geometry or provider).

The upstream explorer does not use the views yet. QLever only substitutes a view on its own when a
query matches the view's pattern exactly, which none of the explorer queries do, so the explorer has
to reference the views explicitly the way the rewrites do to get the speedup.

| view | rewrite of | contains |
|---|---|---|
| `explorer-datasets` | `getDatasets`, `getDatasetCount` | one row per location/dataset/variable with every column `getDatasets` returns |
| `explorer-type-counts` | `getTypes` | number of datasets per mainstem and location type |
| `explorer-variable-counts` | `getVariablesMeasured` | number of datasets per mainstem and variable |
| `explorer-distribution-counts` | `getDistributionNames` | number of datasets per mainstem and provider name |
| `explorer-site-counts` | `getTotalSites` | number of locations per mainstem |

Every view starts with `?mainstem` since a view is stored sorted by its first column and reading it with
a fixed mainstem is a range scan. The count views are only valid for the explorer's current filters
(just the mainstem); if the explorer starts filtering those counts by type or variable, they need
new views.

```sh
mkdir bench && cd bench && chmod 777 .
python3 ../generate_synthetic_graph.py 500000 | docker run -i --rm -u $(id -u):$(id -g) -v $PWD:/data -w /data \
  --entrypoint /qlever/qlever-index docker.io/adfreiburg/qlever:0.6.0 -i geo -F nt -f - -m 8GB
docker run -d --name qb -u $(id -u):$(id -g) -v $PWD:/data -w /data -p 7002:7002 --entrypoint /qlever/qlever-server \
  docker.io/adfreiburg/qlever:0.6.0 -i geo -p 7002 -a tok -m 6GB -c 3GB -e 3GB -j 8
# write + load each view over HTTP (what `qlever index --materialized-views` does at index time)
for view in $(jq -r 'keys[]' ../../../assets/materialized_views.json); do
  curl -X POST -H 'Content-Type: application/sparql-query' --data "$(jq -r --arg v "$view" '.[$v]' ../../../assets/materialized_views.json)" \
    "localhost:7002/?cmd=write-materialized-view&view-name=$view&timeout=3000s&access-token=tok"
  curl "localhost:7002/?cmd=load-materialized-view&view-name=$view&access-token=tok"
done
python3 ../bench_views.py 7002 tok 1 100 5000 999999   # exits non-zero if any rewrite returns different rows
```

Median of 3 runs with the cache cleared (wall clock, includes the HTTP response). Every rewrite returned
the same rows as the upstream query, including for a mainstem with no sites (`999999`, not shown). Writing
all five views took under 2 s on this graph.

| query | mainstem | rows | upstream | view |
|---|---|---|---|---|
| getDatasets | 1 | 17338 | 0.77 s | 0.22 s (mostly transferring the rows) |
| getDatasets | 100 | 281 | 0.51 s | 0.014 s |
| getDatasets | 5000 | 10 | 0.48 s | 0.008 s |
| getDatasetCount | 1 | 1 | 0.64 s | 0.029 s |
| getDatasetCount | 100 | 1 | 0.64 s | 0.009 s |
| getDatasetCount | 5000 | 1 | 0.56 s | 0.009 s |
| getVariablesMeasured | 1 | 300 | 0.099 s | 0.002 s |
| getVariablesMeasured | 100 | 245 | 0.048 s | 0.002 s |
| getDistributionNames | 1 | 15 | 0.058 s | 0.002 s |
| getDistributionNames | 100 | 15 | 0.039 s | 0.002 s |
| getTypes | 1 | 8 | 0.037 s | 0.002 s |
| getTypes | 100 | 8 | 0.024 s | 0.002 s |
| getTotalSites | 1 | 1 | 0.014 s | 0.001 s |
| getTotalSites | 100 | 1 | 0.009 s | 0.001 s |

`getDatasets`/`getDatasetCount` are slow upstream because the `VALUES ?variableMeasured` / `?type` filters make
the planner join ~480k rows first and filter by mainstem last; the others are already fast but become a single
range scan.
