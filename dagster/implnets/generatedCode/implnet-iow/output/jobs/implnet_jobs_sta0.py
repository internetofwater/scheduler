from dagster import job

from ops.implnet_ops_sta0 import harvest_sta0

@job
def implnet_job_sta0():
    harvest_sta0()