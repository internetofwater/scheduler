from dagster import job

from ops.implnet_ops_ndwr0 import harvest_ndwr0

@job
def implnet_job_ndwr0():
    harvest_ndwr0()