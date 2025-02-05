# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from typing import Optional
import lakefs
from lakefs import Client

from userCode.lib.env import (
    LAKEFS_ACCESS_KEY_ID,
    LAKEFS_ENDPOINT_URL,
    LAKEFS_SECRET_ACCESS_KEY,
)


def assert_file_exists(file_path: str, branch_name: str = "main"):
    """Assert that a file path has a valid file within the lakefs cluster"""
    lakefs_client = Client(
        host=LAKEFS_ENDPOINT_URL,
        username=LAKEFS_ACCESS_KEY_ID,
        password=LAKEFS_SECRET_ACCESS_KEY,
    )
    files = list(
        lakefs.repository("geoconnex", client=lakefs_client)
        .branch(branch_name)
        .objects()
    )
    for file in files:
        if file.path == file_path:
            return
    else:
        raise Exception(
            f"{file_path} does not exist in branch {branch_name} which has files {files}"
        )


def delete_file_on_main(file_path: str, branch_to_stage_from: str = "develop"):
    """Delete a file in a staging branch then merge it into main"""
    lakefs_client = Client(
        host=LAKEFS_ENDPOINT_URL,
        username=LAKEFS_ACCESS_KEY_ID,
        password=LAKEFS_SECRET_ACCESS_KEY,
    )
    stagingBranch = lakefs.repository("geoconnex", client=lakefs_client).branch(
        branch_to_stage_from
    )

    allobjs = stagingBranch.objects()
    assert stagingBranch.object(
        file_path
    ).exists(), f"{file_path} does not exist but it should. Branch {branch_to_stage_from} instead contains {list(allobjs)}"

    stagingBranch.object(file_path).delete()

    stagingBranch.commit(message=f"Deleting {file_path}")

    assert not stagingBranch.object(file_path).exists()

    stagingBranch.merge_into("main")


def delete_branch_on_lakefs(branch_name: str):
    lakefs_client = Client(
        host=LAKEFS_ENDPOINT_URL,
        username=LAKEFS_ACCESS_KEY_ID,
        password=LAKEFS_SECRET_ACCESS_KEY,
    )
    lakefs.repository("geoconnex", client=lakefs_client).branch(branch_name).delete()


def create_branch_if_not_exists(branch_name: str) -> lakefs.Branch:
    """Create a branch on the lakefs cluster if it doesn't exist"""
    client = Client(
        host=LAKEFS_ENDPOINT_URL,
        username=LAKEFS_ACCESS_KEY_ID,
        password=LAKEFS_SECRET_ACCESS_KEY,
    )
    branches = list(lakefs.repository("geoconnex", client=client).branches())

    for branch in branches:
        if branch.id == branch_name:
            return branch

    newBranch = (
        lakefs.repository("geoconnex", client=client)
        .branch(branch_name)
        .create(source_reference="main")
    )

    return newBranch


def get_branch(branch_name: str) -> Optional[lakefs.Branch]:
    """Get a reference to a branch on the lakefs cluster"""

    client = Client(
        host=LAKEFS_ENDPOINT_URL,
        username=LAKEFS_ACCESS_KEY_ID,
        password=LAKEFS_SECRET_ACCESS_KEY,
    )

    branches = list(lakefs.repository("geoconnex", client=client).branches())

    for branch in branches:
        if branch.id == branch_name:
            return branch


def move_file(branch: str, source: str, destination: str):
    """Move a file within a given branch from one source to another"""
    lakefs_client = Client(
        host=LAKEFS_ENDPOINT_URL,
        username=LAKEFS_ACCESS_KEY_ID,
        password=LAKEFS_SECRET_ACCESS_KEY,
    )
    remoteBranch = lakefs.repository("geoconnex", client=lakefs_client).branch(branch)

    obj = remoteBranch.object(source)
    obj.copy(branch, destination)
    obj.delete()

    remoteBranch.commit(message=f"Moving {source} to {destination}")


def merge_into_main(branch: str):
    """Merge a branch into the main branch of the lakefs cluster"""
    lakefs_client = Client(
        host=LAKEFS_ENDPOINT_URL,
        username=LAKEFS_ACCESS_KEY_ID,
        password=LAKEFS_SECRET_ACCESS_KEY,
    )
    lakefs.repository("geoconnex", client=lakefs_client).branch(branch).merge_into(
        lakefs.repository("geoconnex", client=lakefs_client).branch("main")
    )
