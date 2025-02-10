from userCode.lib.env import RUNNING_AS_TEST_OR_DEV


def test_test_env_is_detected():
    assert RUNNING_AS_TEST_OR_DEV()
