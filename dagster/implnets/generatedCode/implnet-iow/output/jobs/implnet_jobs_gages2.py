from dagster import job

from ops.implnet_ops_gages2 import harvest_gages2

@job
def implnet_job_gages2():
    harvest_gages2()