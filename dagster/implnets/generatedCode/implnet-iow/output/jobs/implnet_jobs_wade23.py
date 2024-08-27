from dagster import job

from ops.implnet_ops_wade23 import harvest_wade23

@job
def implnet_job_wade23():
    harvest_wade23()