from dagster import job

from ops.implnet_ops_iowusbrrise0 import harvest_iowusbrrise0

@job
def implnet_job_iowusbrrise0():
    harvest_iowusbrrise0()
