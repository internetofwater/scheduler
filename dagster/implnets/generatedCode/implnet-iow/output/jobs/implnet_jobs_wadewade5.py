from dagster import job

from ops.implnet_ops_wadewade5 import harvest_wadewade5

@job
def implnet_job_wadewade5():
    harvest_wadewade5()
