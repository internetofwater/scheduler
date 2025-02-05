#!/bin/sh
# Copyright 2025 Company, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Create an entrypoint for the code server. This could be either dagster dev with debugpy
# for debugging or just dagster code-server for running in production

set -x

if [ "$DAGSTER_DEBUG" = "true" ]; then
  echo "Starting dagster in debug mode and waiting for connection to debugpy"
  exec python -m debugpy --configure-subProcess true --listen 0.0.0.0:5678 -m dagster dev -h 0.0.0.0 -p 3000 -m userCode.main
else
  echo "Starting dagster code server"
  exec dagster code-server start -h 0.0.0.0 -p 4000 -m userCode.main
fi
