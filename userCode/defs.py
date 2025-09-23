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

from userCode import (
    instance,
)
from userCode.assetGroups import (
    config,
    export,
    harvest,
    index_generator,
    release_graph_generator,
    sync,
)
from userCode.assetGroups.harvest import (
    docker_client_environment,
    sitemap_partitions,
)
from userCode.lib.dagster import slack_error_fn
from userCode.lib.env import (
    RUNNING_AS_TEST_OR_DEV,
    strict_env,
)

"""
This file defines all of the core dagster functionality
for starting, monitoring, or initializing the 
pipeline for Geoconnex. 
"""

setup_config_job = define_asset_job(
    "setup_config",
    description="setup the config for the pipeline",
    selection=AssetSelection.groups(config.CONFIG_GROUP),
)

harvest_and_sync_job = define_asset_job(
    "harvest_and_sync",
    description="harvest a source and sync against the live geoconnex graphdb",
    selection=AssetSelection.groups(
        harvest.HARVEST_GROUP,
        sync.SYNC_GROUP,
    ),
)

export_job = define_asset_job(
    "export_nquads",
    description="export the graphdb as nquads to all partner endpoints",
    selection=AssetSelection.groups(export.EXPORT_GROUP)
    # don't automatically run the merge into the main branch of lakefs
    - AssetSelection.assets(export.merge_lakefs_branch_into_main),
)

generate_release_graph_job = define_asset_job(
    "harvest_and_release_as_nq",
    description="harvest a source and generate a release graph nq file in s3",
    selection=AssetSelection.groups(
        harvest.HARVEST_GROUP,
        release_graph_generator.RELEASE_GRAPH_GENERATOR_GROUP,
    ),
)

index_generator_job = define_asset_job(
    "index_generator",
    description="generate an index for qlever",
    selection=AssetSelection.groups(index_generator.INDEX_GEN_GROUP),
)


@schedule(
    cron_schedule="@weekly",
    job=harvest_and_sync_job,
    default_status=DefaultScheduleStatus.STOPPED,
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
defs = Definitions(
    assets=load_assets_from_modules(
        [
            instance,
            harvest,
            export,
            sync,
            config,
            release_graph_generator,
            index_generator,
        ]
    ),
    schedules=[crawl_entire_graph_schedule],
    asset_checks=load_asset_checks_from_modules(
        [
            instance,
            harvest,
            export,
            sync,
            config,
            release_graph_generator,
            index_generator,
        ]
    ),
    jobs=[
        harvest_and_sync_job,
        export_job,
        setup_config_job,
        generate_release_graph_job,
        index_generator_job,
    ],
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
