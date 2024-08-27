from dagster import job

from ops.implnet_ops_wadewade16 import harvest_wadewade16

@job
def implnet_job_wadewade16():
    harvest_wadewade16()
