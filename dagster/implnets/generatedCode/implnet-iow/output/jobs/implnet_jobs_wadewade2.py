from dagster import job

from ops.implnet_ops_wadewade2 import harvest_wadewade2

@job
def implnet_job_wadewade2():
    harvest_wadewade2()
