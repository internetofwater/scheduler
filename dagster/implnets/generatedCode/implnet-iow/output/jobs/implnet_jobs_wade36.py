from dagster import job

from ops.implnet_ops_wade36 import harvest_wade36

@job
def implnet_job_wade36():
    harvest_wade36()