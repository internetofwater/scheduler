from dagster import job

from ops.implnet_ops_wadewade8 import harvest_wadewade8

@job
def implnet_job_wadewade8():
    harvest_wadewade8()
