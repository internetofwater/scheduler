from dagster import job

from ops.implnet_ops_wadewade34 import harvest_wadewade34

@job
def implnet_job_wadewade34():
    harvest_wadewade34()
