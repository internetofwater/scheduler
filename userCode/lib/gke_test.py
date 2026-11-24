# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import pytest

from userCode.lib.gke import rollout_complete


def deployment(
    generation=2, observed=2, updated=1, replicas=1, available=1, conditions=None
):
    return {
        "metadata": {"name": "qlever", "generation": generation},
        "spec": {"replicas": 1},
        "status": {
            "observedGeneration": observed,
            "updatedReplicas": updated,
            "replicas": replicas,
            "availableReplicas": available,
            "conditions": conditions or [],
        },
    }


def test_rollout_complete():
    assert rollout_complete(deployment())


def test_rollout_not_observed_yet():
    assert not rollout_complete(deployment(observed=1))


def test_rollout_pod_not_available_yet():
    assert not rollout_complete(deployment(available=0))


def test_rollout_old_pod_still_running():
    assert not rollout_complete(deployment(updated=0))


def test_rollout_progress_deadline_exceeded():
    with pytest.raises(RuntimeError):
        rollout_complete(
            deployment(
                available=0,
                conditions=[
                    {"type": "Progressing", "reason": "ProgressDeadlineExceeded"}
                ],
            )
        )
