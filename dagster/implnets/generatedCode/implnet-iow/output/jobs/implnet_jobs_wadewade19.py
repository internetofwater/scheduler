from dagster import job

from ops.implnet_ops_wadewade19 import harvest_wadewade19

@job
def implnet_job_wadewade19():
    harvest_wadewade19()
