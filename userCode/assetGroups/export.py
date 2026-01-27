# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


from dagster import (
    AssetExecutionContext,
    asset,
    get_dagster_logger,
)

from userCode.assetGroups.index_generator import INDEX_DIRECTORY
from userCode.lib.classes import RcloneClient, S3
from userCode.lib.dagster import all_dependencies_materialized
from userCode.lib.env import (
    RUNNING_AS_TEST_OR_DEV,
)
from userCode.lib.lakefs import LakeFSClient

"""
This file defines all geoconenx exports that move data
outside of the triplestore. 

"""

EXPORT_GROUP = "exports"


RELEASE_GRAPH_LOCATION_IN_S3 = "graphs/latest/"

DEVELOPMENT_BRANCH_IN_LAKEFS = "develop"


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
    group_name=EXPORT_GROUP,
)
def stream_all_release_graphs_to_renci(
    context: AssetExecutionContext,
    rclone_config: str,
):
    """
    Stream all release graphs to RENCI
    """
    if RUNNING_AS_TEST_OR_DEV():
        get_dagster_logger().warning("Skipping export as we are running in test mode")
        return
    lakefs_client = LakeFSClient("geoconnex")
    get_dagster_logger().info(
        f"Uploading release graphs from {RELEASE_GRAPH_LOCATION_IN_S3} to lakefs at {DEVELOPMENT_BRANCH_IN_LAKEFS}"
    )
    RcloneClient(rclone_config).copy_directory_to_lakefs(
        destination_branch=DEVELOPMENT_BRANCH_IN_LAKEFS,
        source_prefix=RELEASE_GRAPH_LOCATION_IN_S3,
        lakefs_client=lakefs_client,
    )


@asset(group_name=EXPORT_GROUP)
def stream_qlever_index_to_gcs(context: AssetExecutionContext):
    """
    Stream all files in the generated qlever index to GCS
    """
    if RUNNING_AS_TEST_OR_DEV():
        get_dagster_logger().warning("Skipping export as we are running in test mode")
        return
    s3 = S3()
    for file in INDEX_DIRECTORY.iterdir():
        if not file.is_file():
            raise Exception(f"{file} is not a file and thus cannot be uploaded")

        get_dagster_logger().info(
            f"Uploading {file.name} of size {file.stat().st_size} to the object store"
        )
        with file.open("rb") as f:
            s3.load_stream(
                stream=f,
                remote_path=f"geoconnex_index/{file.name}",
                content_length=file.stat().st_size,
                content_type="octet-stream",
                headers={},
            )


@asset(group_name=EXPORT_GROUP)
def merge_lakefs_branch_into_main(context: AssetExecutionContext):
    """
    Manually merge the develop branch into the main branch
    the renci lakefs. This is done as a separate step to avoid
    auto merging unfinished or incorrect assets until they have been
    checked
    """
    if RUNNING_AS_TEST_OR_DEV():
        get_dagster_logger().warning("Skipping export as we are running in test mode")
        return
    LakeFSClient("geoconnex").merge_branch_into_main(branch="develop")


# @asset(group_name=EXPORT_GROUP)
# def stream_nquads_to_zenodo(
#     context: AssetExecutionContext,
# ):
#     """Upload nquads to Zenodo as a new deposit"""
#     # check if we are running in test mode and thus want to upload to the sandbox
#     SANDBOX_MODE = (
#         ZENODO_SANDBOX_ACCESS_TOKEN != "unset" and "PYTEST_CURRENT_TEST" in os.environ
#     )

#     if (
#         skip_export(context)
#         # if we are running against a test sandbox, allow the user to upload
#         and not SANDBOX_MODE
#     ):
#         return

#     ZENODO_API_URL = (
#         "https://zenodo.org/api/deposit/depositions"
#         if not SANDBOX_MODE
#         else "https://sandbox.zenodo.org/api/deposit/depositions"
#     )

#     if SANDBOX_MODE:
#         ZENODO_API_URL = "https://sandbox.zenodo.org/api/deposit/depositions"
#         TOKEN = ZENODO_SANDBOX_ACCESS_TOKEN
#     else:
#         ZENODO_API_URL = "https://zenodo.org/api/deposit/depositions"
#         TOKEN = ZENODO_ACCESS_TOKEN

#     headers = {
#         "Authorization": f"Bearer {TOKEN}",
#         "Content-Type": "application/json",
#     }

#     # Create a new deposit
#     response = requests.post(ZENODO_API_URL, json={}, headers=headers)
#     response.raise_for_status()
#     deposit = response.json()

#     # Extract Deposit ID
#     deposit_id = deposit["id"]
#     get_dagster_logger().info(f"Deposit created with ID: {deposit_id}")

#     # Read file stream from S3
#     # we are not decoding the content to upsert it as gzip to zenodo
#     stream = S3().read_stream(export_graphdb_as_nquads, decode_content=False)

#     # Use the deposit ID to upload the file
#     response = requests.put(
#         deposit["links"]["bucket"] + "/geoconnex-graph.nq.gz",
#         data=stream,
#         headers={"Authorization": f"Bearer {TOKEN}"},
#     )

#     response.raise_for_status()

#     get_dagster_logger().info("File uploaded successfully.")

#     # Add metadata to the upload
#     metadata = {
#         "metadata": {
#             "title": "Geoconnex Graph",
#             "upload_type": "dataset",
#             "description": (
#                 "This file represents the n-quads export of all content "
#                 "contained in the Geoconnex graph database. Documentation "
#                 "and background can be found at https://geoconnex.us"
#             ),
#             "creators": [
#                 {
#                     "name": "Internet of Water Coalition",
#                     "affiliation": "Internet of Water Coalition",
#                 }
#             ],
#         }
#     }

#     metadata_url = f"{ZENODO_API_URL}/{deposit_id}"
#     response = requests.put(metadata_url, json=metadata, headers=headers)
#     response.raise_for_status()

#     get_dagster_logger().info(f"Metadata updated for deposit ID {deposit_id}")

#     """
#     In zenodo you cannot delete a deposit after it has been published.
#     Thus, the code below is commented out. It is safer not to automatically
#     publish the deposit. However the code below is tested and works.
#     """
#     # publish the deposit; thus making it no longer tagged as a draft
#     # publish_url = f"{ZENODO_API_URL}/{deposit_id}/actions/publish"
#     # response = requests.post(publish_url, headers=headers)
#     # response.raise_for_status()
#     # get_dagster_logger().info("Deposit published successfully.")
#     # return deposit_id
