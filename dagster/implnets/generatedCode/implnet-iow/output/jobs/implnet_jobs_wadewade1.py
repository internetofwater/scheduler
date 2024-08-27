from dagster import job

from ops.implnet_ops_wadewade1 import harvest_wadewade1

@job
def implnet_job_wadewade1():
    harvest_wadewade1()
