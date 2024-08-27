from dagster import job

from ops.implnet_ops_wade22 import harvest_wade22

@job
def implnet_job_wade22():
    harvest_wade22()