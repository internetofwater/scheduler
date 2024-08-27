from dagster import job

from ops.implnet_ops_iowlinks0 import harvest_iowlinks0

@job
def implnet_job_iowlinks0():
    harvest_iowlinks0()
