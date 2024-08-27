from dagster import job

from ops.implnet_ops_wadewade14 import harvest_wadewade14

@job
def implnet_job_wadewade14():
    harvest_wadewade14()
