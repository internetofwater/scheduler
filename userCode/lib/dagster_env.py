from dagster import DynamicPartitionsDefinition

# This is the list of sources to crawl that is dynamically generated at runtime by parsing the geoconnex config
sources_partitions_def = DynamicPartitionsDefinition(name="sources_partitions_def")
