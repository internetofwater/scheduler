# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import concurrent.futures
from datetime import datetime
import os
from typing import Optional
from dagster import (
    AssetExecutionContext,
    asset,
    get_dagster_logger,
)
import requests
import tempfile
from userCode.lib.classes import S3, RcloneClient
from userCode.lib.dagster import all_dependencies_materialized
from userCode.lib.env import (
    GLEANER_GRAPH_URL,
    GLEANERIO_DATAGRAPH_ENDPOINT,
    RUNNING_AS_TEST_OR_DEV,
    ZENODO_ACCESS_TOKEN,
    ZENODO_SANDBOX_ACCESS_TOKEN,
)
from userCode.lib.lakefs import LakeFSClient
from userCode.pipeline import finished_individual_crawl

"""
This file defines all geoconenx exports that move data
outside of the triplestore. 

"""


def skip_export(context: AssetExecutionContext) -> bool:
    """Skip export if all dependencies are not materialized or we are running in test mode"""
    if not all_dependencies_materialized(context, "finished_individual_crawl"):
        get_dagster_logger().warning(
            "Skipping export as all dependencies are not materialized"
        )
        return True
    if RUNNING_AS_TEST_OR_DEV():
        get_dagster_logger().warning(
            "Dependencies are materialized, but skipping export as we are running in test mode"
        )
        return True
    return False


@asset(
    deps=[finished_individual_crawl],
    group_name="exports",
)
def export_graph_as_nquads(context: AssetExecutionContext) -> Optional[str]:
    """Export the graphdb to nquads"""
    if skip_export(context):
        return

    base_url = (
        GLEANER_GRAPH_URL if not RUNNING_AS_TEST_OR_DEV() else "http://localhost:7200"
    )

    # Define the repository name and endpoint
    endpoint = f"{base_url}/repositories/{GLEANERIO_DATAGRAPH_ENDPOINT}"
    size_endpoint = f"{base_url}/repositories/iow/size"  # Endpoint for graph size
    query = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"

    headers = {
        "Content-Type": "application/sparql-query",
        "Accept": "application/n-quads",
    }

    # Get the size of the graph
    get_dagster_logger().info(f"Fetching graph size from {size_endpoint}")
    response = requests.get(size_endpoint)
    response.raise_for_status()
    total_data_size = response.json()

    get_dagster_logger().info(f"Graph size: {total_data_size} triples")

    # Create a temporary file to accumulate the nquads
    with tempfile.NamedTemporaryFile() as tmp_file:
        tmp_filename = tmp_file.name
        get_dagster_logger().info(
            f"Using temporary file {tmp_filename} for concatenation"
        )

        # Split the download work into chunks (based on the total size)
        chunk_size = 10000000  # Number of triples per chunk, adjust accordingly
        chunk_offsets = range(0, total_data_size, chunk_size)

        # Function to handle streaming and appending each chunk to the file
        def process_chunk(offset: int):
            query_with_offset = f"{query} OFFSET {offset} LIMIT {chunk_size}"

            # Sending request for a chunk of data
            with requests.post(
                endpoint, headers=headers, data=query_with_offset, stream=True
            ) as r:
                r.raise_for_status()

                # Append the chunk to the temporary file
                with open(
                    tmp_filename, "ab"
                ) as tmp_file:  # 'ab' mode for appending in binary mode
                    tmp_file.write(r.raw.read())

                get_dagster_logger().info(
                    f"Chunk starting at OFFSET {offset} added to file"
                )

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_offset = {
                executor.submit(process_chunk, offset): offset
                for offset in chunk_offsets
            }

            # Wait for all futures to complete
            concurrent.futures.wait(future_to_offset)

        # After all chunks are processed, upload the final file to S3
        s3_client = S3()
        filename = f"backups/nquads_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.nq"

        # Upload the concatenated file
        with open(tmp_filename, "rb") as tmp_file:
            s3_client.load_stream(tmp_file, filename)

        get_dagster_logger().info(f"Export completed, file saved as {filename}")

        return filename


@asset(
    group_name="exports",
)
def nquads_to_renci(
    context: AssetExecutionContext,
    rclone_config: str,
    export_graph_as_nquads: Optional[str],  # contains the path to the nquads
):
    """Upload the nquads to the renci bucket in lakefs"""
    if skip_export(context) or not export_graph_as_nquads:
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
    group_name="exports",
)
def nquads_to_zenodo(
    context: AssetExecutionContext,
    export_graph_as_nquads: Optional[str],
):
    """Upload nquads to Zenodo as a new deposit"""
    # check if we are running in test mode and thus want to upload to the sandbox
    SANDBOX_MODE = (
        ZENODO_SANDBOX_ACCESS_TOKEN != "unset" and "PYTEST_CURRENT_TEST" in os.environ
    )

    if (
        skip_export(context)
        # if we are running against a test sandbox, allow the user to upload
        and not SANDBOX_MODE
    ) or (not export_graph_as_nquads):
        return

    # Read file stream from S3
    stream = S3().read_stream(export_graph_as_nquads)

    ZENODO_API_URL = (
        "https://zenodo.org/api/deposit/depositions"
        if not SANDBOX_MODE
        else "https://sandbox.zenodo.org/api/deposit/depositions"
    )

    if SANDBOX_MODE:
        ZENODO_API_URL = "https://sandbox.zenodo.org/api/deposit/depositions"
        TOKEN = ZENODO_SANDBOX_ACCESS_TOKEN
    else:
        ZENODO_API_URL = "https://zenodo.org/api/deposit/depositions"
        TOKEN = ZENODO_ACCESS_TOKEN

    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
    }

    # Create a new deposit
    response = requests.post(ZENODO_API_URL, json={}, headers=headers)
    response.raise_for_status()
    deposit = response.json()

    # Extract Deposit ID
    deposit_id = deposit["id"]
    get_dagster_logger().info(f"Deposit created with ID: {deposit_id}")

    # Use the deposit ID to upload the file
    response = requests.post(
        f"{ZENODO_API_URL}/{deposit_id}/files",
        files={"file": ("nquads.nt", stream)},
        headers={"Authorization": f"Bearer {TOKEN}"},
    )

    response.raise_for_status()

    get_dagster_logger().info("File uploaded successfully.")

    # Add metadata to the upload
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

    get_dagster_logger().info(f"Metadata updated for deposit ID {deposit_id}")

    """
    In zenodo you cannot delete a deposit after it has been published.
    Thus, the code below is commented out. It is safer not to automatically
    publish the deposit. However the code below is tested and works.
    """
    # publish the deposit; thus making it no longer tagged as a draft
    # publish_url = f"{ZENODO_API_URL}/{deposit_id}/actions/publish"
    # response = requests.post(publish_url, headers=headers)
    # response.raise_for_status()
    # get_dagster_logger().info("Deposit published successfully.")
    # return deposit_id
