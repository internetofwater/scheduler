from dagster import job

from ops.implnet_ops_wadewade40 import harvest_wadewade40

@job
def implnet_job_wadewade40():
    harvest_wadewade40()
