from dagster import job

from ops.implnet_ops_wadewade36 import harvest_wadewade36

@job
def implnet_job_wadewade36():
    harvest_wadewade36()
