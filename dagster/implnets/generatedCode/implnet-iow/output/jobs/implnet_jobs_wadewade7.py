from dagster import job

from ops.implnet_ops_wadewade7 import harvest_wadewade7

@job
def implnet_job_wadewade7():
    harvest_wadewade7()
