# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import os
from userCode.lib.env import RUNNING_AS_TEST_OR_DEV, userCodeRoot


def test_test_env_is_detected():
    assert RUNNING_AS_TEST_OR_DEV()


def test_user_code_root():
    path = os.path.join(userCodeRoot, "main.py")
    assert os.path.isfile(path)

    path = os.path.join(userCodeRoot, "NON_EXISTENT.py")
    assert not os.path.isfile(path)
