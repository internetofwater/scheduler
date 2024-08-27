from dagster import job

from ops.implnet_ops_wadewade32 import harvest_wadewade32

@job
def implnet_job_wadewade32():
    harvest_wadewade32()
