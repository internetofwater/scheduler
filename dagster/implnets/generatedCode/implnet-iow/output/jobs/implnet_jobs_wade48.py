from dagster import job

from ops.implnet_ops_wade48 import harvest_wade48

@job
def implnet_job_wade48():
    harvest_wade48()