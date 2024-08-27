from dagster import job

from ops.implnet_ops_wade42 import harvest_wade42

@job
def implnet_job_wade42():
    harvest_wade42()