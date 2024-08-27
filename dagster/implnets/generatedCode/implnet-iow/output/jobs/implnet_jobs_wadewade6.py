from dagster import job

from ops.implnet_ops_wadewade6 import harvest_wadewade6

@job
def implnet_job_wadewade6():
    harvest_wadewade6()
