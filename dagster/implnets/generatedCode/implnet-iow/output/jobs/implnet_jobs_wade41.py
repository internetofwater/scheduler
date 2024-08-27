from dagster import job

from ops.implnet_ops_wade41 import harvest_wade41

@job
def implnet_job_wade41():
    harvest_wade41()