from dagster import job

from ops.implnet_ops_wadewade45 import harvest_wadewade45

@job
def implnet_job_wadewade45():
    harvest_wadewade45()
