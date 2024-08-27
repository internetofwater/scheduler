from dagster import job

from ops.implnet_ops_wade38 import harvest_wade38

@job
def implnet_job_wade38():
    harvest_wade38()