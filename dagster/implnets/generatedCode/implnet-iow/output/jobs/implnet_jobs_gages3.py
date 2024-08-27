from dagster import job

from ops.implnet_ops_gages3 import harvest_gages3

@job
def implnet_job_gages3():
    harvest_gages3()