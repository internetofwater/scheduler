from dagster import job

from ops.implnet_ops_wadewade13 import harvest_wadewade13

@job
def implnet_job_wadewade13():
    harvest_wadewade13()
