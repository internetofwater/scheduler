from dagster import job

from ops.implnet_ops_wadewade12 import harvest_wadewade12

@job
def implnet_job_wadewade12():
    harvest_wadewade12()
