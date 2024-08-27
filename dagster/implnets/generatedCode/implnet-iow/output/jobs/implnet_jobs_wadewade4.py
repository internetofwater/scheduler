from dagster import job

from ops.implnet_ops_wadewade4 import harvest_wadewade4

@job
def implnet_job_wadewade4():
    harvest_wadewade4()
