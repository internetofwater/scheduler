from dagster import job

from ops.implnet_ops_wadewade3 import harvest_wadewade3

@job
def implnet_job_wadewade3():
    harvest_wadewade3()
