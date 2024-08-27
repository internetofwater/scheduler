from dagster import job

from ops.implnet_ops_wadewade30 import harvest_wadewade30

@job
def implnet_job_wadewade30():
    harvest_wadewade30()
