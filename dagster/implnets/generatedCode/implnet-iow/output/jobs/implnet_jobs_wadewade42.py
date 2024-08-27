from dagster import job

from ops.implnet_ops_wadewade42 import harvest_wadewade42

@job
def implnet_job_wadewade42():
    harvest_wadewade42()
