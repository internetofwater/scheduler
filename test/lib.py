# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import os

from userCode.lib.classes import RcloneClient

"""
All functions in this file are helpers for testing
and are not needed in the main pipeline
"""


def assert_rclone_config_is_accessible():
    location = RcloneClient.get_config_path()
    assert location.parent.exists(), f"{location} does not exist"
    assert os.system(f"{RcloneClient.get_bin()} version") == 0
