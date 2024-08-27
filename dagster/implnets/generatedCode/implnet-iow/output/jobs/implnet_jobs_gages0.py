from dagster import job

from ops.implnet_ops_gages0 import harvest_gages0

@job
def implnet_job_gages0():
    harvest_gages0()