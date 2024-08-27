from dagster import job

from ops.implnet_ops_wadewade49 import harvest_wadewade49

@job
def implnet_job_wadewade49():
    harvest_wadewade49()
