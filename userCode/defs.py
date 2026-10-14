# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0


from collections.abc import Iterator

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    DynamicPartitionsDefinition,
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
    processed,
)
from userCode.assetGroups.harvest import (
    docker_client_environment,
    sitemap_partitions,
)
from userCode.lib.dagster import (
    bulk_sources_partitions_def,
    slack_error_fn,
    sources_partitions_def,
)
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

export_job = define_asset_job(
    "export_artifacts",
    description="export artifacts like index dumps, nquads, and geoparquet to partners and other storage locations",
    selection=AssetSelection.groups(export.EXPORT_GROUP),
)

# feature and bulk harvests are separate jobs since they use
# different partitions and dagster jobs can only have one partitions definition
harvest_job = define_asset_job(
    "harvest_and_generate_parquet",
    description="harvest a source and generate a parquet file of its jsonld in s3",
    selection=AssetSelection.assets(harvest.harvest_sitemap),
)

# processed datasets are expensive to generate and their upstream sources
# rarely change, so this job is run by hand rather than on a schedule
processed_job = define_asset_job(
    "generate_processed_data",
    description="derive datasets like the catchment to mainstem lookup file from upstream data products and publish them to processed/ in s3",
    selection=AssetSelection.groups(processed.PROCESSED_GROUP),
)

harvest_bulk_job = define_asset_job(
    "harvest_bulk_and_generate_parquet",
    description="harvest a bulk source by running its bulk container and generate a parquet file of its jsonld in s3",
    selection=AssetSelection.assets(harvest.harvest_bulk_sitemap),
)


def harvest_run_requests(
    context: ScheduleEvaluationContext,
    job_name: str,
    partitions_def: DynamicPartitionsDefinition,
) -> Iterator[RunRequest]:
    """Set up the environment and create a run of the harvest job for every partition"""
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

    assert partitions_def.name
    partition_keys = context.instance.get_dynamic_partitions(partitions_def.name)
    context.log.info(f"Found partition keys: {partition_keys}")

    if not partition_keys:
        raise Exception("No partition keys found after materializing environment!")

    for partition_key in partition_keys:
        context.log.info(f"Creating run for {partition_key}")
        yield RunRequest(
            job_name=job_name,
            run_key=partition_key,
            partition_key=partition_key,
            tags={"run_type": "harvest_weekly"},
        )


@schedule(
    cron_schedule="@monthly",
    job=harvest_job,
    default_status=DefaultScheduleStatus.STOPPED,
)
def crawl_entire_graph_schedule(context: ScheduleEvaluationContext):
    yield from harvest_run_requests(context, harvest_job.name, sources_partitions_def)


@schedule(
    cron_schedule="@monthly",
    job=harvest_bulk_job,
    default_status=DefaultScheduleStatus.STOPPED,
)
def crawl_bulk_sources_schedule(context: ScheduleEvaluationContext):
    yield from harvest_run_requests(
        context, harvest_bulk_job.name, bulk_sources_partitions_def
    )


# expose all the code needed for our dagster repo
defs = Definitions(
    assets=load_assets_from_modules(
        [
            instance,
            harvest,
            export,
            config,
            processed,
        ]
    ),
    schedules=[
        crawl_entire_graph_schedule,
        crawl_bulk_sources_schedule,
    ],
    asset_checks=load_asset_checks_from_modules(
        [
            instance,
            harvest,
            export,
            config,
            processed,
        ]
    ),
    jobs=[
        export_job,
        setup_config_job,
        harvest_job,
        harvest_bulk_job,
        processed_job,
    ],
    sensors=[
        dagster_slack.make_slack_on_run_failure_sensor(
            channel="#cgs-iow-bots",
            slack_token=strict_env("DAGSTER_SLACK_TOKEN"),
            text_fn=slack_error_fn,
            default_status=DefaultSensorStatus.STOPPED
            if RUNNING_AS_TEST_OR_DEV()
            else DefaultSensorStatus.RUNNING,
            monitor_all_repositories=True,
        )
    ],
    # Can be used to send arbitrary slack messages
    resources={
        "slack": dagster_slack.SlackResource(token=strict_env("DAGSTER_SLACK_TOKEN")),
    },
)
