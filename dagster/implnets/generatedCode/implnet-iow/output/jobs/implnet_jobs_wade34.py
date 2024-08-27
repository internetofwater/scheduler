from dagster import job

from ops.implnet_ops_wade34 import harvest_wade34

@job
def implnet_job_wade34():
    harvest_wade34()