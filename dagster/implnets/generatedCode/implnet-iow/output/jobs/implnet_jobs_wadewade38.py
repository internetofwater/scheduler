from dagster import job

from ops.implnet_ops_wadewade38 import harvest_wadewade38

@job
def implnet_job_wadewade38():
    harvest_wadewade38()
