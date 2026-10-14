#!/usr/bin/env bash
# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# Runs on a laptop to deploy the latest main to the scheduler VM without
# `tofu apply`. Waits for the GHCR image build of the current origin/main commit
# to finish, then SSHes into the VM and runs `make cloudUpdate` there.
#
# The VM name and zone default to the `vm_name`/`vm_zone` tofu outputs and can be
# overridden with VM_NAME and VM_ZONE. FORCE=true is passed through to cloudUpdate.

set -euo pipefail

cd "$(dirname "$0")/.."

WORKFLOW="push_to_ghcr.yml"
BRANCH="main"
FORCE="${FORCE:-false}"

VM_NAME="${VM_NAME:-$(tofu -chdir=opentofu output -raw vm_name 2>/dev/null || true)}"
VM_ZONE="${VM_ZONE:-$(tofu -chdir=opentofu output -raw vm_zone 2>/dev/null || true)}"
if [[ -z "$VM_NAME" || -z "$VM_ZONE" ]]; then
    echo "Could not read vm_name/vm_zone from tofu outputs; set VM_NAME and VM_ZONE" >&2
    exit 1
fi

git fetch --quiet origin "$BRANCH"
commit=$(git rev-parse "origin/$BRANCH")
echo "Deploying $(git log -1 --format='%h %s' "$commit") to $VM_NAME ($VM_ZONE)"

if [[ "$(git rev-parse HEAD)" != "$commit" ]]; then
    echo "NOTE: your local HEAD differs from origin/$BRANCH; only pushed commits are deployed"
fi

# the workflow run for a fresh push can take a few seconds to appear
run_id=""
for _ in $(seq 1 12); do
    run_id=$(gh run list --workflow "$WORKFLOW" --branch "$BRANCH" --commit "$commit" \
        --limit 1 --json databaseId --jq '.[0].databaseId // empty')
    [[ -n "$run_id" ]] && break
    echo "Waiting for a $WORKFLOW run for $commit to start"
    sleep 5
done
if [[ -z "$run_id" ]]; then
    echo "No $WORKFLOW run found for $commit; was it pushed to $BRANCH?" >&2
    exit 1
fi

echo "Waiting for the image build to finish (run $run_id)"
gh run watch "$run_id" --exit-status --interval 15

gcloud compute ssh "$VM_NAME" --zone "$VM_ZONE" \
    --command "cd /opt/scheduler && sudo FORCE=$FORCE make cloudUpdate"
