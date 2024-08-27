from dagster import job

from ops.implnet_ops_wade46 import harvest_wade46

@job
def implnet_job_wade46():
    harvest_wade46()