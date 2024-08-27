from dagster import job

from ops.implnet_ops_wadewade33 import harvest_wadewade33

@job
def implnet_job_wadewade33():
    harvest_wadewade33()
