from dagster import job

from ops.implnet_ops_wadewade23 import harvest_wadewade23

@job
def implnet_job_wadewade23():
    harvest_wadewade23()
