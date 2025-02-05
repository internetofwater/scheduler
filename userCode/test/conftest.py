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

import os
import pytest


# These are needed given the fact that the tests are always best ran outside the docker
# container. We set this here since we don't want to put it in the .env necessarily
@pytest.fixture(scope="session", autouse=True)
def setup_before_tests():
    # assume we are not inside the compose project if we are testing and thus
    # we want to use localhost to connect. We have to set this here since
    # we don't want to put it in the .env necessarily
    os.environ["DAGSTER_POSTGRES_HOST"] = "localhost"
