from dagster import job

from ops.implnet_ops_wade24 import harvest_wade24

@job
def implnet_job_wade24():
    harvest_wade24()