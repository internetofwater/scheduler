from dagster import job

from ops.implnet_ops_wade25 import harvest_wade25

@job
def implnet_job_wade25():
    harvest_wade25()