# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

"""
A minimal client for restarting a Deployment on a GKE cluster.

It authenticates as the service account of the GCE VM dagster runs on, so it
only works in prod; the account needs permission to update deployments in the
cluster (e.g. roles/container.developer).
"""

import base64
from dataclasses import dataclass
from datetime import datetime, timezone
import tempfile
import time

from dagster import get_dagster_logger
import requests

METADATA_TOKEN_URL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
GKE_API = "https://container.googleapis.com/v1"
# the annotation `kubectl rollout restart` sets to roll the pods
RESTARTED_AT_ANNOTATION = "kubectl.kubernetes.io/restartedAt"


def access_token() -> str:
    """Get an OAuth access token for the VM's service account from the GCE metadata server"""
    response = requests.get(
        METADATA_TOKEN_URL, headers={"Metadata-Flavor": "Google"}, timeout=10
    )
    response.raise_for_status()
    return response.json()["access_token"]


def rollout_complete(deployment: dict) -> bool:
    """Whether a deployment has finished rolling out, using the same rules as `kubectl rollout status`"""
    metadata = deployment.get("metadata", {})
    spec = deployment.get("spec", {})
    status = deployment.get("status", {})
    if status.get("observedGeneration", 0) < metadata.get("generation", 0):
        return False
    for condition in status.get("conditions", []):
        if (
            condition.get("type") == "Progressing"
            and condition.get("reason") == "ProgressDeadlineExceeded"
        ):
            raise RuntimeError(
                f"Rollout of deployment {metadata.get('name')} exceeded its progress deadline"
            )
    replicas = spec.get("replicas", 1)
    return (
        status.get("updatedReplicas", 0) == replicas
        and status.get("replicas", 0) == replicas
        and status.get("availableReplicas", 0) == replicas
    )


@dataclass
class GKEDeployment:
    project: str
    zone: str
    cluster: str
    namespace: str
    name: str

    def _cluster_connection(self, token: str) -> tuple[str, str]:
        """Return the cluster's API server URL and a path to its CA certificate"""
        response = requests.get(
            f"{GKE_API}/projects/{self.project}/locations/{self.zone}/clusters/{self.cluster}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        response.raise_for_status()
        cluster = response.json()
        ca_file = tempfile.NamedTemporaryFile(
            prefix="gke-ca-", suffix=".crt", delete=False
        )
        ca_file.write(base64.b64decode(cluster["masterAuth"]["clusterCaCertificate"]))
        ca_file.close()
        return f"https://{cluster['endpoint']}", ca_file.name

    def _deployment_url(self, server: str) -> str:
        return (
            f"{server}/apis/apps/v1/namespaces/{self.namespace}/deployments/{self.name}"
        )

    def check_access(self) -> dict:
        """
        Connect to the cluster, read the deployment, and confirm we are allowed
        to restart it, without changing anything. Returns the deployment's status
        """
        logger = get_dagster_logger()
        token = access_token()
        server, ca_file = self._cluster_connection(token)
        logger.info(f"Connected to the {self.cluster} API server at {server}")

        response = requests.get(
            self._deployment_url(server),
            headers={"Authorization": f"Bearer {token}"},
            verify=ca_file,
            timeout=30,
        )
        response.raise_for_status()
        status = response.json().get("status", {})
        logger.info(f"Read deployment {self.namespace}/{self.name}: {status}")

        # ask the API server whether a restart (a patch) would be allowed
        response = requests.post(
            f"{server}/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SelfSubjectAccessReview",
                "spec": {
                    "resourceAttributes": {
                        "group": "apps",
                        "resource": "deployments",
                        "verb": "patch",
                        "namespace": self.namespace,
                        "name": self.name,
                    }
                },
            },
            verify=ca_file,
            timeout=30,
        )
        response.raise_for_status()
        review = response.json().get("status", {})
        if not review.get("allowed"):
            raise PermissionError(
                f"Not allowed to patch deployment {self.namespace}/{self.name}: "
                f"{review.get('reason') or review.get('evaluationError') or 'no reason given'}"
            )
        logger.info(f"Allowed to restart deployment {self.namespace}/{self.name}")
        return status

    def restart(
        self, timeout_seconds: int = 3600, poll_interval_seconds: int = 30
    ) -> None:
        """
        Restart the deployment like `kubectl rollout restart` and block until
        the new pod is available
        """
        logger = get_dagster_logger()
        token = access_token()
        server, ca_file = self._cluster_connection(token)
        url = self._deployment_url(server)

        restarted_at = datetime.now(timezone.utc).isoformat()
        logger.info(
            f"Restarting deployment {self.namespace}/{self.name} on {self.cluster}"
        )
        response = requests.patch(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/strategic-merge-patch+json",
            },
            json={
                "spec": {
                    "template": {
                        "metadata": {
                            "annotations": {RESTARTED_AT_ANNOTATION: restarted_at}
                        }
                    }
                }
            },
            verify=ca_file,
            timeout=30,
        )
        response.raise_for_status()

        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            time.sleep(poll_interval_seconds)
            # tokens last an hour, so get a fresh one for each poll
            response = requests.get(
                url,
                headers={"Authorization": f"Bearer {access_token()}"},
                verify=ca_file,
                timeout=30,
            )
            response.raise_for_status()
            deployment = response.json()
            if rollout_complete(deployment):
                logger.info(f"Deployment {self.namespace}/{self.name} is available")
                return
            status = deployment.get("status", {})
            logger.info(
                f"Waiting for rollout: {status.get('updatedReplicas', 0)} updated, "
                f"{status.get('availableReplicas', 0)} available"
            )

        raise TimeoutError(
            f"Deployment {self.namespace}/{self.name} did not become available within {timeout_seconds}s"
        )
