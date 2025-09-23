#!/bin/sh
# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


# Create an entrypoint for the code server. This could be either dagster dev with debugpy
# for debugging or just dagster code-server for running in production

set -x

if [ "$DAGSTER_DEBUG" = "true" ]; then
  echo "Starting dagster in debug mode and waiting for connection to debugpy"
  exec python -m debugpy --configure-subProcess true --listen 0.0.0.0:5678 -m dagster dev -h 0.0.0.0 -p 3000 -m userCode.defs
else
  echo "Starting dagster code server"
  exec dagster code-server start -h 0.0.0.0 -p 4000 -m userCode.defs
fi
