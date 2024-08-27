from dagster import job

from ops.implnet_ops_wadewade25 import harvest_wadewade25

@job
def implnet_job_wadewade25():
    harvest_wadewade25()
