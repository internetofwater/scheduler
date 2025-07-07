# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

FROM ghcr.io/oras-project/oras:v1.2.2 AS oras

WORKDIR /workspace

RUN mkdir -p /workspace/oras_cache

# mount the oras cache into the container to use this from host 
ENV ORAS_CACHE=/workspace/oras_cache

RUN mkdir -p /workspace/graphs

RUN oras pull ghcr.io/internetofwater/geoconnex-graph:latest

FROM adfreiburg/qlever:commit-55c05d4

COPY --from=oras /workspace/geoconnex_graph geoconnex_graph

RUN echo '{ "num-triples-per-batch": 100000 }' > geoconnex.settings.json
RUN cat geoconnex_graph/*.nq | IndexBuilderMain -i geoconnex -s geoconnex.settings.json -F nq -f -

ENTRYPOINT ["bash", "-c", "ServerMain -i geoconnex -j 8 -p 8888 -m 5G -c 2G -e 1G -k 200 -s 30s"]