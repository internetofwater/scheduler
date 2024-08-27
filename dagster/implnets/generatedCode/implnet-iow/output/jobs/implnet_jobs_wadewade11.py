from dagster import job

from ops.implnet_ops_wadewade11 import harvest_wadewade11

@job
def implnet_job_wadewade11():
    harvest_wadewade11()
