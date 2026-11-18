# QLever materialized view benchmark

Compares the explorer queries (`userCode/assetGroups/queries/explorer/*.sparql`)
with and without the materialized views in `assets/materialized_views.json`,
on a synthetic graph with the same shape as geoconnex monitoring locations
(500k locations, ~14M triples, skewed so `mainstems/1` is large).

```sh
mkdir bench && cd bench && chmod 777 .
python3 ../generate_synthetic_graph.py 500000 | docker run -i --rm -u $(id -u):$(id -g) -v $PWD:/data -w /data \
  --entrypoint /qlever/qlever-index docker.io/adfreiburg/qlever:0.6.0 -i geo -F nt -f - -m 8GB
# baseline: server without the views
docker run -d --name qb -u $(id -u):$(id -g) -v $PWD:/data -w /data -p 7002:7002 --entrypoint /qlever/qlever-server \
  docker.io/adfreiburg/qlever:0.6.0 -i geo -p 7002 -a tok -m 6GB -c 3GB -e 3GB -j 8
python3 ../bench_queries.py 7002 baseline 1 100 5000
# write + load the view over HTTP (what `qlever index --materialized-views` does at index time)
curl -X POST -H 'Content-Type: application/sparql-query' --data "$(jq -r '.["explorer-datasets"]' ../../../assets/materialized_views.json)" \
  'localhost:7002/?cmd=write-materialized-view&view-name=explorer-datasets&timeout=3000s&access-token=tok'
curl 'localhost:7002/?cmd=load-materialized-view&view-name=explorer-datasets&access-token=tok'
python3 ../bench_explicit_view.py 7002 1 100 5000   # prints a result hash to compare with the baseline
```

Median of 3 runs with the cache cleared (wall clock, includes the HTTP response); result hashes were identical
to the baseline for every query:

| query | mainstem | baseline | explicit view |
|---|---|---|---|
| getDatasetCount | 1 (19k datasets) | 0.76 s | 0.021 s |
| getDatasetCount | 100 | 0.75 s | 0.008 s |
| getDatasetCount | 5000 | 0.84 s | 0.008 s |
| getDatasets | 1 (19k rows) | 0.85 s | 0.23 s (mostly transferring 19k rows) |
| getDatasets | 100 | 0.66 s | 0.011 s |
| getDatasets | 5000 | 0.65 s | 0.007 s |

The baseline is slow because the `VALUES ?variableMeasured` / `?type` filters make the planner join ~480k
rows first and filter by mainstem last. The other four queries are already 10-70 ms and the
views QLever applied to them automatically only saved ~25%, so only `explorer-datasets` is kept.

QLever only substitutes a view automatically when the query plan matches the view's plan exactly, and
it did not for `getDatasets`. The explorer has to reference the view explicitly to get the speedup, as in
`userCode/assetGroups/queries/explorer_datasets_materialized_view.sparql`.
