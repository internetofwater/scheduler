# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import os
from userCode.lib.classes import S3, RcloneClient


def test_get_rclone_bin():
    binary = RcloneClient.get_bin()
    assert binary
    assert os.path.exists(binary)
    assert os.system(f"{binary} --version") == 0


def test_s3_load():
    S3().load(b"test", "key1")
    assert b"test" == S3().read("key1")
    assert S3().object_has_content("key1")

    S3().load(b"", "key2")
    assert not S3().object_has_content("key2")


def test_s3_read_stream():
    longData = b"test" * 1024 * 1024
    S3().load(longData, "streamKey")
    assert S3().object_has_content("streamKey")

    stream = S3().read_stream("streamKey")
    assert stream is not None
    data = b""
    for chunk in stream:
        data += chunk
    assert data == longData
