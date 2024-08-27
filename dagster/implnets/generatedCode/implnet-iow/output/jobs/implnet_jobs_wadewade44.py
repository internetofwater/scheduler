from dagster import job

from ops.implnet_ops_wadewade44 import harvest_wadewade44

@job
def implnet_job_wadewade44():
    harvest_wadewade44()
