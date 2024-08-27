from dagster import job

from ops.implnet_ops_wadewade9 import harvest_wadewade9

@job
def implnet_job_wadewade9():
    harvest_wadewade9()
