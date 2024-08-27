from dagster import job

from ops.implnet_ops_wade29 import harvest_wade29

@job
def implnet_job_wade29():
    harvest_wade29()