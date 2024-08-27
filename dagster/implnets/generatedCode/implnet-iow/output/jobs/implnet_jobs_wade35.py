from dagster import job

from ops.implnet_ops_wade35 import harvest_wade35

@job
def implnet_job_wade35():
    harvest_wade35()