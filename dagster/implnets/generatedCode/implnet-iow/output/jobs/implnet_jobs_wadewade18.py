from dagster import job

from ops.implnet_ops_wadewade18 import harvest_wadewade18

@job
def implnet_job_wadewade18():
    harvest_wadewade18()
