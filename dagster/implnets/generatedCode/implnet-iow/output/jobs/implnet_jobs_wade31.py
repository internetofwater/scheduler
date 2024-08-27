from dagster import job

from ops.implnet_ops_wade31 import harvest_wade31

@job
def implnet_job_wade31():
    harvest_wade31()