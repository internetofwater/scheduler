from dagster import job

from ops.implnet_ops_wadewade10 import harvest_wadewade10

@job
def implnet_job_wadewade10():
    harvest_wadewade10()
