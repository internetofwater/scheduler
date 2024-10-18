#!/bin/sh

# Create an entrypoint for the code server. This could be either dagster dev with debugpy
# for debugging or just dagster code-server for running in production

set -x
if [ "$DAGSTER_DEBUG" != "true" ] && [ "$DAGSTER_DEBUG" != "false" ]; then
  echo "DAGSTER_DEBUG env var must be set to one of 'true' or 'false'"
  exit 1
fi

if [ "$DAGSTER_DEBUG" = "true" ]; then
  echo "Starting dagster in debug mode and waiting for connection to debugpy"
  exec python -m debugpy --configure-subProcess true --listen 0.0.0.0:5678 -m dagster dev -h 0.0.0.0 -p 3000 --python-file /opt/dagster/app/code/main.py -d /opt/dagster/app/code
else
  echo "Starting dagster code server"
  exec dagster code-server start -h 0.0.0.0 -p 4000 --python-file /opt/dagster/app/code/main.py -d /opt/dagster/app/code
fi
