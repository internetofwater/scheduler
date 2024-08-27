from dagster import job

from ops.implnet_ops_wadewade26 import harvest_wadewade26

@job
def implnet_job_wadewade26():
    harvest_wadewade26()
