from dagster import job

from ops.implnet_ops_wadewade24 import harvest_wadewade24

@job
def implnet_job_wadewade24():
    harvest_wadewade24()
