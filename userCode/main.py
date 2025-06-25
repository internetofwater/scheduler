# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    load_asset_checks_from_modules,
    load_assets_from_modules,
    materialize,
    schedule,
)
import dagster_slack

from userCode import instance
from userCode.pipeline import (
    docker_client_environment,
    sitemap_partitions,
)

from . import exports, pipeline
from .lib.dagster import slack_error_fn
from .lib.env import (
    RUNNING_AS_TEST_OR_DEV,
    strict_env,
)

"""
This file defines all of the core dagster functionality
for starting, monitoring, or initializing the 
pipeline for Geoconnex. 
"""


harvest_job = define_asset_job(
    "harvest_source",
    description="harvest a source for the geoconnex graphdb",
    selection=AssetSelection.all() - AssetSelection.groups("exports"),
)

export_job = define_asset_job(
    "export_nquads",
    description="export the graphdb as nquads to all partner endpoints",
    selection=AssetSelection.groups("exports"),
)


@schedule(
    cron_schedule="@weekly",
    job=harvest_job,
    default_status=DefaultScheduleStatus.STOPPED
    if RUNNING_AS_TEST_OR_DEV()
    else DefaultScheduleStatus.RUNNING,
)
def crawl_entire_graph_schedule(context: ScheduleEvaluationContext):
    context.log.info("Schedule triggered.")

    result = materialize(
        [
            sitemap_partitions,
            docker_client_environment,
        ],
        instance=context.instance,
    )
    if not result.success:
        raise Exception(f"Failed to materialize environment assets!: {result}")

    partition_keys = context.instance.get_dynamic_partitions("sources_partitions_def")
    context.log.info(f"Found partition keys: {partition_keys}")

    if not partition_keys:
        raise Exception("No partition keys found after materializing environment!")

    for partition_key in partition_keys:
        context.log.info(f"Creating run for {partition_key}")
        yield RunRequest(
            job_name="harvest_source",
            run_key=partition_key,
            partition_key=partition_key,
            tags={"run_type": "harvest_weekly"},
        )


# expose all the code needed for our dagster repo
definitions = Definitions(
    assets=load_assets_from_modules([pipeline, exports, instance]),
    schedules=[crawl_entire_graph_schedule],
    asset_checks=load_asset_checks_from_modules([pipeline, exports, instance]),
    jobs=[harvest_job, export_job],
    sensors=[
        dagster_slack.make_slack_on_run_failure_sensor(
            channel="#cgs-iow-bots",
            slack_token=strict_env("DAGSTER_SLACK_TOKEN"),
            text_fn=slack_error_fn,
            default_status=DefaultSensorStatus.STOPPED
            if RUNNING_AS_TEST_OR_DEV()
            else DefaultSensorStatus.RUNNING,
            monitor_all_code_locations=True,
        )
    ],
    # Commented out but can uncomment if we want to send other slack msgs
    # resources={
    #     "slack": dagster_slack.SlackResource(token=strict_env("DAGSTER_SLACK_TOKEN")),
    # },
)
