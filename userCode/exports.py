# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from typing import Optional
from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    asset,
    get_dagster_logger,
)
import requests
from userCode.lib.classes import S3, RcloneClient
from userCode.lib.dagster import all_dependencies_materialized
from userCode.lib.env import (
    GLEANER_GRAPH_URL,
    GLEANERIO_DATAGRAPH_ENDPOINT,
    RUNNING_AS_TEST_OR_DEV,
    ZENODO_ACCESS_TOKEN,
)
from userCode.lib.lakefs import LakeFSClient
from userCode.pipeline import finished_individual_crawl

"""
This file defines all geoconenx exports that move data
outside of the triplestore. 

"""


run_once_at_end_of_pipeline: AutomationCondition = (
    # only run automatically if all dependencies are materialized
    AutomationCondition.all_deps_match(
        ~AutomationCondition.missing().since_last_handled()
    ).newly_true()
    # only run automatically if no backfill is in progress
    & AutomationCondition.all_deps_match(
        ~AutomationCondition.backfill_in_progress().since_last_handled()
    ).newly_true()
    # only run automatically if it is part of a pipeline in progress
    & AutomationCondition.in_progress().since_last_handled().newly_true()
)


@asset(
    deps=[finished_individual_crawl],
    automation_condition=run_once_at_end_of_pipeline,
)
def export_graph_as_nquads(context: AssetExecutionContext) -> Optional[str]:
    """Export the graphdb to nquads"""

    if not all_dependencies_materialized(context, "finished_individual_crawl"):
        get_dagster_logger().warning(
            "Skipping nquads export as all dependencies are not materialized"
        )
        return

    base_url = (
        GLEANER_GRAPH_URL if not RUNNING_AS_TEST_OR_DEV() else "http://localhost:7200"
    )

    # Define the repository name and endpoint
    endpoint = (
        f"{base_url}/repositories/{GLEANERIO_DATAGRAPH_ENDPOINT}/statements?infer=false"
    )

    get_dagster_logger().info(
        f"Exporting graphdb to nquads; fetching data from {endpoint}"
    )
    # Download the nq export
    response = requests.get(
        endpoint,
        headers={
            "Accept": "application/n-quads",
        },
    )

    # Check if the request was successful
    if response.status_code == 200:
        # Save the response content to a file
        with open("outputfile.nq", "wb") as f:
            f.write(response.content)
        get_dagster_logger().info("Export of graphdb to nquads successful")
    else:
        raise RuntimeError(
            f"Export failed, status code: {response.status_code} with response {response.text}"
        )

    s3_client = S3()
    filename = f"backups/nquads_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}"
    s3_client.load(response.content, filename)

    return filename


@asset(
    automation_condition=run_once_at_end_of_pipeline,
)
def nquads_to_renci(
    context: AssetExecutionContext,
    rclone_config: str,
    export_graph_as_nquads: Optional[str],  # contains the path to the nquads
):
    """Upload the nquads to the renci bucket in lakefs"""
    if (
        not all_dependencies_materialized(context, "finished_individual_crawl")
        or not export_graph_as_nquads
    ):
        get_dagster_logger().warning(
            "Skipping rclone copy as all dependencies are not materialized"
        )
        return
    elif RUNNING_AS_TEST_OR_DEV():
        get_dagster_logger().warning(
            "Skipping rclone copy as we are running in test mode"
        )
        return

    rclone_client = RcloneClient(rclone_config)
    lakefs_client = LakeFSClient("geoconnex")

    rclone_client.copy_to_lakefs(
        destination_branch="develop",
        destination_filename="iow-dump.nq",
        path_to_file=export_graph_as_nquads,
        lakefs_client=lakefs_client,
    )
    lakefs_client.merge_branch_into_main(branch="develop")


@asset(
    automation_condition=run_once_at_end_of_pipeline,
)
def nquads_to_zenodo(
    context: AssetExecutionContext, export_graph_as_nquads: Optional[str]
):
    """Upload nquads to zenodo"""
    if (
        not all_dependencies_materialized(context, "finished_individual_crawl")
        or not export_graph_as_nquads
    ):
        get_dagster_logger().warning(
            "Skipping zenodo copy as all dependencies are not materialized"
        )
        return
    elif RUNNING_AS_TEST_OR_DEV():
        get_dagster_logger().warning(
            "Skipping zenodo copy as we are running in test mode"
        )
        return

    ZENODO_API_URL = "https://zenodo.org/api/deposit/depositions"

    headers = {
        "Authorization": f"Bearer {ZENODO_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    response = requests.post(ZENODO_API_URL, json={}, headers=headers)
    response.raise_for_status()  # Ensure request was successful
    deposit = response.json()

    # Extract Deposit ID
    deposit_id = deposit["id"]
    get_dagster_logger().info(f"Deposit created with ID: {deposit_id}")

    with open("outputfile.nq", "rb") as f:
        files = {"file": f}

        upload_url = f"{ZENODO_API_URL}/{deposit_id}/files"
        response = requests.post(
            upload_url,
            files=files,
            headers={"Authorization": f"Bearer {ZENODO_ACCESS_TOKEN}"},
        )
        response.raise_for_status()  # Ensure request was successful

    get_dagster_logger().info("File uploaded successfully.")

    metadata = {
        "metadata": {
            "title": "Geoconnex Graph",
            "upload_type": "dataset",
            "description": "This file represents the n-quads export of all content contained in the Geoconnex graph database. Documentation and background can be found at https://docs.geoconnex.us/",
            "creators": [
                {
                    "name": "Internet of Water Coalition",
                    "affiliation": "Internet of Water Coalition",
                }
            ],
        }
    }

    metadata_url = f"{ZENODO_API_URL}/{deposit_id}"
    response = requests.put(metadata_url, json=metadata, headers=headers)
    response.raise_for_status()


# Publish the deposit (optional)
# publish_url = f"{ZENODO_API_URL}/{deposit_id}/actions/publish"
# response = requests.post(publish_url, headers=headers)
# response.raise_for_status()
# get_dagster_logger().info("Deposit published successfully.")
