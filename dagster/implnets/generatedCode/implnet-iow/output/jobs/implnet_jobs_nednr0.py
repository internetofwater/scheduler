from dagster import job

from ops.implnet_ops_nednr0 import harvest_nednr0

@job
def implnet_job_nednr0():
    harvest_nednr0()