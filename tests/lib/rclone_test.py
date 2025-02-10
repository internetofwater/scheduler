import os


def test_rclone_installed():
    """make sure you can run rclone version"""
    assert os.system("rclone version") == 0
