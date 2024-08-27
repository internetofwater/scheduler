from dagster import job

from ops.implnet_ops_wadewade46 import harvest_wadewade46

@job
def implnet_job_wadewade46():
    harvest_wadewade46()
