#!/usr/bin/env bash
# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# Runs on the scheduler VM to deploy the latest code and images without
# recreating the VM. Pulls the repo, pulls the latest GHCR images, and recreates
# only the containers whose image or config changed. Invoked as `sudo make cloudUpdate`
# on the VM, or remotely via `make redeploy` from a laptop.
#
# Dagster runs execute inside the user code container, so restarting it kills any
# in-progress run; this refuses to update while runs are active unless FORCE=true.

set -euo pipefail

cd "$(dirname "$0")/.."

FORCE="${FORCE:-false}"
DAGSTER_GRAPHQL_URL="${DAGSTER_GRAPHQL_URL:-http://localhost:3000/graphql}"

# QUEUED runs live in postgres and survive a restart, so only these are at risk
ACTIVE_RUNS_QUERY='{"query":"{ runsOrError(filter: {statuses: [STARTING, STARTED, CANCELING]}) { ... on Runs { results { runId jobName status } } } }"}'

if [[ "$FORCE" != "true" ]]; then
    echo "Checking for in-progress Dagster runs"
    if response=$(curl -sf --max-time 10 -H "Content-Type: application/json" \
        --data "$ACTIVE_RUNS_QUERY" "$DAGSTER_GRAPHQL_URL"); then
        active_runs=$(python3 -c '
import json, sys
runs = json.load(sys.stdin)["data"]["runsOrError"]["results"]
for run in runs:
    print("  " + "  ".join(run[key] for key in ("runId", "jobName", "status")))
' <<<"$response")
        if [[ -n "$active_runs" ]]; then
            echo "Refusing to update; these runs would be killed by restarting user code:"
            echo "$active_runs"
            echo "Wait for them to finish, or rerun with FORCE=true"
            exit 1
        fi
    else
        echo "WARNING: could not reach the Dagster webserver at $DAGSTER_GRAPHQL_URL;" \
            "continuing without checking for in-progress runs"
    fi
fi

previous_commit=$(git rev-parse HEAD)
git pull --ff-only
if [[ "$previous_commit" != "$(git rev-parse HEAD)" ]]; then
    echo "Pulled new commits:"
    git log --oneline "$previous_commit..HEAD"
else
    echo "Repo already up to date at $(git rev-parse --short HEAD)"
fi

# re-invoke make so any makefile changes that were just pulled take effect
make cloudPull
make cloudProd ARGS="--remove-orphans"

# drop the images the new ones replaced so the disk doesn't fill up over time
docker image prune -f

echo "Running containers:"
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}"
