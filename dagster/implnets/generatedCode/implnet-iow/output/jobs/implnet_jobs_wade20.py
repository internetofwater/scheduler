from dagster import job

from ops.implnet_ops_wade20 import harvest_wade20

@job
def implnet_job_wade20():
    harvest_wade20()