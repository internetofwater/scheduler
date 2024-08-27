from dagster import job

from ops.implnet_ops_wadewade31 import harvest_wadewade31

@job
def implnet_job_wadewade31():
    harvest_wadewade31()
