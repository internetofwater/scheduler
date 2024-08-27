from dagster import job

from ops.implnet_ops_wade30 import harvest_wade30

@job
def implnet_job_wade30():
    harvest_wade30()