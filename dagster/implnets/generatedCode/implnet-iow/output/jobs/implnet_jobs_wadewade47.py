from dagster import job

from ops.implnet_ops_wadewade47 import harvest_wadewade47

@job
def implnet_job_wadewade47():
    harvest_wadewade47()
