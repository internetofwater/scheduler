from dagster import job

from ops.implnet_ops_wade33 import harvest_wade33

@job
def implnet_job_wade33():
    harvest_wade33()