# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import os


def test_rclone_installed():
    """make sure you can run rclone version"""
    assert os.system("rclone version") == 0
