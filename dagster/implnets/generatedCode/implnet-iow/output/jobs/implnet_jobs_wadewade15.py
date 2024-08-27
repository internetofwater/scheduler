from dagster import job

from ops.implnet_ops_wadewade15 import harvest_wadewade15

@job
def implnet_job_wadewade15():
    harvest_wadewade15()
