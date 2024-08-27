from dagster import job

from ops.implnet_ops_wadewade22 import harvest_wadewade22

@job
def implnet_job_wadewade22():
    harvest_wadewade22()
