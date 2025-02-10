# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from typing import Optional
from dagster import OpExecutionContext, asset, get_dagster_logger
import requests
from userCode.lib.classes import S3, FileTransferer
from userCode.lib.dagster import all_dependencies_materialized
from userCode.lib.env import (
    GLEANER_GRAPH_URL,
    GLEANERIO_DATAGRAPH_ENDPOINT,
    RUNNING_AS_TEST_OR_DEV,
)
from userCode.lib.lakefs import LakeFSClient
from userCode.pipeline import finished_individual_crawl


"""
This file defines all geoconenx exports that move data
outside of the triplestore. 

"""


@asset(deps=[finished_individual_crawl])
def export_graph_as_nquads(context: OpExecutionContext) -> Optional[str]:
    """Export the graphdb to nquads"""

    if not all_dependencies_materialized(context, "finished_individual_crawl"):
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


@asset()
def nquads_to_renci(
    context: OpExecutionContext,
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

    if RUNNING_AS_TEST_OR_DEV():
        get_dagster_logger().warning(
            "Skipping rclone copy as we are running in test mode"
        )
        return

    rclone_client = FileTransferer(rclone_config)
    lakefs_client = LakeFSClient("geoconnex")

    rclone_client.copy_to_lakefs(
        destination_branch="develop",
        destination_filename="iow-dump.nq",
        path_to_file=export_graph_as_nquads,
        lakefs_client=lakefs_client,
    )
    lakefs_client.merge_branch_into_main(branch="develop")
