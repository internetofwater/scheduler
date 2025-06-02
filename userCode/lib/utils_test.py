# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import os
from userCode.lib.utils import create_max_length_container_name


def test_create_max_length_container_name():
    assert create_max_length_container_name("foo", "bar") == "foo_bar"
    assert create_max_length_container_name("foo", "barbaz") == "foo_barbaz"

    result = create_max_length_container_name("nabu_org", "superlongname" * 100)
    assert len(result) <= 63
    assert "superlongname" in result


def test_ensure_rclone_bin_installed():
    assert os.system("rclone version") == 0
