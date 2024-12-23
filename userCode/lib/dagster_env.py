from dagster import DagsterInstance, DynamicPartitionsDefinition, get_dagster_logger

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
