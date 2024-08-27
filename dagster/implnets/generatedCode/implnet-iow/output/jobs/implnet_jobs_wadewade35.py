from dagster import job

from ops.implnet_ops_wadewade35 import harvest_wadewade35

@job
def implnet_job_wadewade35():
    harvest_wadewade35()
