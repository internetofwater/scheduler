from dagster import job

from ops.implnet_ops_usbrrise0 import harvest_usbrrise0

@job
def implnet_job_usbrrise0():
    harvest_usbrrise0()
