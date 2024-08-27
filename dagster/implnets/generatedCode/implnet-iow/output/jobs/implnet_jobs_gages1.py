from dagster import job

from ops.implnet_ops_gages1 import harvest_gages1

@job
def implnet_job_gages1():
    harvest_gages1()