from dagster import job

from ops.implnet_ops_wade27 import harvest_wade27

@job
def implnet_job_wade27():
    harvest_wade27()