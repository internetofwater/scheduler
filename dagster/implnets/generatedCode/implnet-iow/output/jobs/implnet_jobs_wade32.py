from dagster import job

from ops.implnet_ops_wade32 import harvest_wade32

@job
def implnet_job_wade32():
    harvest_wade32()