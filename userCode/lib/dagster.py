# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import json
from dagster import (
    AssetExecutionContext,
    AssetKey,
    DagsterInstance,
    DynamicPartitionsDefinition,
    RunFailureSensorContext,
    get_dagster_logger,
)

# This is the list of sources to crawl that is dynamically generated at runtime by parsing the geoconnex config
sources_partitions_def = DynamicPartitionsDefinition(name="sources_partitions_def")


def filter_partitions(
    instance: DagsterInstance, partition_name: str, keys_to_keep: set[str]
) -> None:
    """Remove all old partitions that are not in the list of keys to keep.
    This is needed since dagster does not remove old partitions but keeps them by default for historical monitoring"""
    sources_partitions_def = instance.get_dynamic_partitions(partition_name)
    for key in sources_partitions_def:
        if key not in keys_to_keep:
            get_dagster_logger().info(
                f"Deleting partition: {key} from {partition_name}"
            )
            instance.delete_dynamic_partition(partition_name, key)


def dagster_log_with_parsed_level(structural_log_message: str) -> None:
    """Try to structurally parse the log message and log it with the appropriate level"""
    try:
        json_log = json.loads(structural_log_message)
    except json.JSONDecodeError:
        get_dagster_logger().info(structural_log_message)
        return

    level = json_log["level"]
    if not level:
        get_dagster_logger().info(structural_log_message)
        return
    msg = json_log["msg"]
    match level:
        case "info":
            get_dagster_logger().info(msg)
        case "warning" | "warn":
            get_dagster_logger().warning(msg)
        case "error":
            get_dagster_logger().error(msg)
        case "debug":
            get_dagster_logger().debug(msg)
        case "trace":
            # dagster does not have a trace level
            get_dagster_logger().debug(msg)
        case _:
            get_dagster_logger().info(msg)

    return


def all_dependencies_materialized(
    context: AssetExecutionContext, dependency_asset_key: str
) -> bool:
    """Check if all partitions of a given asset are materialized"""
    instance = context.instance
    all_partitions = sources_partitions_def.get_partition_keys(
        dynamic_partitions_store=instance
    )
    # Check if all partitions of finished_individual_crawl are materialized
    materialized_partitions = context.instance.get_materialized_partitions(
        asset_key=AssetKey(dependency_asset_key)
    )

    if len(all_partitions) != len(materialized_partitions):
        get_dagster_logger().warning(
            f"Not all partitions of {dependency_asset_key} are materialized, so nq generation will be skipped"
        )
        return False
    else:
        get_dagster_logger().info(
            f"All partitions of {dependency_asset_key} are detected as having been materialized"
        )
        return True


def slack_error_fn(context: RunFailureSensorContext) -> str:
    get_dagster_logger().info("Sending notification to Slack")
    # The make_slack_on_run_failure_sensor automatically sends the job
    # id and name so you can just send the error. We don't need other data in the string
    source_being_crawled = context.partition_key
    if source_being_crawled:
        return f"Error for partition: {source_being_crawled}: {context.failure_event.message}"
    else:
        return f"Error: {context.failure_event.message}"
