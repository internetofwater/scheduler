# Scheduler

This repository uses Dagster to run Nabu and Gleaner. Dagster is [asset-oriented](https://dagster.io/blog/software-defined-assets), and thus the graph is built primarily from the linking of assets, not just jobs.

The Geoconnex graph database is made up of the JSON-LD documents from over 200 different organizations in the Geoconnex sitemap. In order to prevent code duplication, we use [partitioned assets](https://docs.dagster.io/concepts/partitions-schedules-sensors/partitioning-assets) in Dagster. These allow the same asset definition to used across all of our sources.

The Geoconnex graph is set up to be crawled at a regular interval by using a [schedule](https://docs.dagster.io/concepts/automation/schedules) cron label. However, to prevent overuse of server resources or spamming target websites, we set the max concurrent run limit. To do this, [we limit concurrency in the dagster.yaml config](https://docs.dagster.io/guides/limiting-concurrency-in-data-pipelines#limiting-overall-runs). However, we still use concurrency within the same job when syncing our graph with the S3 bucket or other local operations.

Sitemaps that are harvested by a bulk container (those with a `geoconnex:bulk_container_image` in the sitemap index) are put in a separate partition and harvested by the `harvest_bulk_sitemap` asset, while all other sitemaps are crawled by the `harvest_sitemap` asset. Each asset uses its own [pool](https://docs.dagster.io/guides/operate/managing-concurrency), and the default pool limit in `dagster.yaml` applies to each pool individually. This allows bulk crawls and feature / API crawls to run at the same time without either type overwhelming a single server.

If the job for an asset generation fails, a slack notification will be sent.

> (NOTE: asset materialization via the UI is not a job, and thus will not trigger a notification)
