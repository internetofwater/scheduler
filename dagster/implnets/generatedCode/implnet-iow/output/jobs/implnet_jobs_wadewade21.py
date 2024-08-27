from dagster import job

from ops.implnet_ops_wadewade21 import harvest_wadewade21

@job
def implnet_job_wadewade21():
    harvest_wadewade21()
