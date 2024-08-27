from dagster import job

from ops.implnet_ops_wadewade20 import harvest_wadewade20

@job
def implnet_job_wadewade20():
    harvest_wadewade20()
