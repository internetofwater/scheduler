from dagster import job

from ops.implnet_ops_wadewade37 import harvest_wadewade37

@job
def implnet_job_wadewade37():
    harvest_wadewade37()
