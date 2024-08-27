from dagster import job

from ops.implnet_ops_wadewade43 import harvest_wadewade43

@job
def implnet_job_wadewade43():
    harvest_wadewade43()
