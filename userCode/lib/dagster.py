# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Sequence
import json

from dagster import (
    AssetExecutionContext,
    AssetKey,
    DagsterInstance,
    DynamicPartitionsDefinition,
    RunFailureSensorContext,
    get_dagster_logger,
)


class AllPartitions:
    _usgs_partition_name = "usgs_partitions_def"
    _generic_partition_name = "generic_partitions_def"

    # This is the list of sources to crawl that is dynamically generated at runtime by parsing the geoconnex config
    usgs_partitions_def = DynamicPartitionsDefinition(name="usgs_partitions_def")
    generic_partition_def = DynamicPartitionsDefinition(name="generic_partitions_def")

    @classmethod
    def partition_names_to_keys(
        cls, instance: DagsterInstance
    ) -> dict[str, Sequence[str]]:
        usgs_partitions = cls.usgs_partitions_def.get_partition_keys(
            dynamic_partitions_store=instance
        )
        generic_partitions = cls.generic_partition_def.get_partition_keys(
            dynamic_partitions_store=instance
        )

        name_to_keys = {
            cls._usgs_partition_name: usgs_partitions,
            cls._generic_partition_name: generic_partitions,
        }
        return name_to_keys

    @classmethod
    def all_dependencies_materialized(
        cls, context: AssetExecutionContext, dependency_asset_key: str
    ) -> bool:
        """Check if all partitions of a given asset are materialized"""
        instance = context.instance

        all_partitions = cls.partition_names_to_keys(instance)

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
        case "warning" | "warn":
            get_dagster_logger().warning(msg)
        case "error" | "fatal":
            get_dagster_logger().error(msg)
        case "debug":
            get_dagster_logger().debug(msg)
        case "trace":
            # dagster does not have a trace level
            get_dagster_logger().debug(msg)
        case "info" | _:
            get_dagster_logger().info(msg)

    return


def slack_error_fn(context: RunFailureSensorContext) -> str:
    get_dagster_logger().info("Sending notification to Slack")
    # The make_slack_on_run_failure_sensor automatically sends the job
    # id and name so you can just send the error. We don't need other data in the string
    source_being_crawled = context.partition_key
    if source_being_crawled:
        return f"Error for partition: {source_being_crawled}: {context.failure_event.message}"
    else:
        return f"Error: {context.failure_event.message}"
