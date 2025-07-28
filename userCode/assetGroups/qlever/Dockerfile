# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

FROM internetofwater/nabu:latest AS nabu

RUN mkdir -p /app/pull/

#  https://stackoverflow.com/questions/76351391/difference-between-cache-to-from-and-mount-type-cache-in-docker-buildx-build/76351422#76351422
# --cache-to/from is used to store the result of a build step and reuse it in future builds, avoiding the need to run the command again. This is stored in a persistent location outside of the builder, like on a registry, so that other builders can skip already completed steps of a build even if the image wasn't built on the local system.
# --mount type=cache creates a mount inside the temporary container that's executed in a RUN step. This mount is reused in later executions of the build when the step itself is not cached. This is useful when a step pulls down a lot of external dependencies that do not need to be in the image and can safely be reused between builds. The storage of the mount cache is local to the builder and is an empty directory on first use.

# Run nabu pull which pulls all release graphs that aren't already on disk to 
# the local directory; we use the cache mount to store the result of the pull on the host
# so that future builds can reuse it; howeverl this is not stored in the image since it is
# not needed for runtime 
RUN --mount=type=cache,target=/app/pull \
    --mount=type=secret,id=S3_ACCESS_KEY \
    --mount=type=secret,id=S3_SECRET_KEY \
    bash -c 'S3_ACCESS_KEY=$(cat /run/secrets/S3_ACCESS_KEY) && \
    S3_SECRET_KEY=$(cat /run/secrets/S3_SECRET_KEY) && \
    /app/nabu pull /tmp/pull/ --prefix graphs/latest/ --s3-access-key "$S3_ACCESS_KEY" \
    --s3-secret-key "$S3_SECRET_KEY" --bucket harvest-geoconnex-us \
    --port 443 --address storage.googleapis.com --ssl'


FROM adfreiburg/qlever:commit-55c05d4

COPY --from=nabu /app/pull geoconnex_graph

RUN echo '{ "num-triples-per-batch": 100000 }' > geoconnex.settings.json
RUN cat geoconnex_graph/*.nq | IndexBuilderMain -i geoconnex -s geoconnex.settings.json -F nq -f -

ENTRYPOINT ["bash", "-c", "ServerMain -i geoconnex -j 8 -p 8888 -m 5G -c 2G -e 1G -k 200 -s 30s"]