from dagster import job

from ops.implnet_ops_wadewade29 import harvest_wadewade29

@job
def implnet_job_wadewade29():
    harvest_wadewade29()
