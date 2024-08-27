from dagster import job

from ops.implnet_ops_SOURCEVAL import harvest_SOURCEVAL

@job
def implnet_job_SOURCEVAL():
    harvest_SOURCEVAL()
