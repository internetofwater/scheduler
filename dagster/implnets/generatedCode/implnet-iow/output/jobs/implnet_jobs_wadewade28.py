from dagster import job

from ops.implnet_ops_wadewade28 import harvest_wadewade28

@job
def implnet_job_wadewade28():
    harvest_wadewade28()
