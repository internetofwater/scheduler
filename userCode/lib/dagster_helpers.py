# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import json
from dagster import (
    DagsterInstance,
    DynamicPartitionsDefinition,
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
