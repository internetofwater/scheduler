# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import datetime

from dagster import AssetExecutionContext, Config, RunsFilter, asset

"""
This file holds assets that interact with or manage
the state of the DagsterInstance; these are mainly
helpers and are not needed in the main pipeline
"""


class DeleteRunConfig(Config):
    # default to a month but this can be changed in the UI
    days: int = 30
    # maximum amount of records to fetch at a time
    max_records: int = 100_000


@asset()
def cleanup_old_run_records(context: AssetExecutionContext, config: DeleteRunConfig):
    """
    Delete old runs and their associated events from storage. Useful for
    ensuring the database doesn't grow indefinitely

    NOTE: For some db's like postgres you may also need to run vaccum after
    to actually reclaim disk space there may be additional
    steps to take depending on the underlying db that is used
    """

    instance = context.instance

    # define the time threshold for what is old enough
    week_ago = datetime.datetime.now() - datetime.timedelta(days=config.days)

    old_run_records = instance.get_run_records(
        filters=RunsFilter(created_before=week_ago),
        limit=config.max_records,  # limit how many are fetched at a time, perform this operation in batches
        ascending=True,  # start from the oldest
    )

    # in this simple asset we delete serially
    # for higher throughput you could parallelize with threads
    for record in old_run_records:
        # delete all the database contents for this run
        instance.delete_run(record.dagster_run.run_id)
