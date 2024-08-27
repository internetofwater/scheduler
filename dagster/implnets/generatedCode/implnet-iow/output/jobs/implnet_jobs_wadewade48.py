from dagster import job

from ops.implnet_ops_wadewade48 import harvest_wadewade48

@job
def implnet_job_wadewade48():
    harvest_wadewade48()
