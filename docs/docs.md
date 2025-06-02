# Scheduler

This repository uses Dagster to run Nabu and Gleaner. Dagster is [asset-oriented](https://dagster.io/blog/software-defined-assets), and thus the graph is built primarily from the linking of assets, not just jobs.

The Geoconnex graph database is made up of the JSON-LD documents from over 200 different organizations in the Geoconnex sitemap. In order to prevent code duplication, we use [partitioned assets](https://docs.dagster.io/concepts/partitions-schedules-sensors/partitioning-assets) in Dagster. These allow the same asset definition to used across all of our sources.

The Geoconnex graph is set up to be crawled at a regular interval by using a [schedule](https://docs.dagster.io/concepts/automation/schedules) cron label. However, to prevent overuse of server resources or spamming target websites, we set the max concurrent run limit. To do this, [we limit concurrency in the dagster.yaml config](https://docs.dagster.io/guides/limiting-concurrency-in-data-pipelines#limiting-overall-runs). However, we still use concurrency within the same job when syncing our graph with the S3 bucket or other local operations.

If the job for an asset generation fails, a slack notification will be sent.

> (NOTE: asset materialization via the UI is not a job, and thus will not trigger a notification)
