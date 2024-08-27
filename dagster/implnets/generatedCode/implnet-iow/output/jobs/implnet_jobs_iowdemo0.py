from dagster import job

from ops.implnet_ops_iowdemo0 import harvest_iowdemo0

@job
def implnet_job_iowdemo0():
    harvest_iowdemo0()
