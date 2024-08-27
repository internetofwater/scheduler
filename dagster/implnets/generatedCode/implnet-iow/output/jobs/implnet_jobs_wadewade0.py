from dagster import job

from ops.implnet_ops_wadewade0 import harvest_wadewade0

@job
def implnet_job_wadewade0():
    harvest_wadewade0()
