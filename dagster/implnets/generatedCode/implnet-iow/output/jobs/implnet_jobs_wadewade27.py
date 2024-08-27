from dagster import job

from ops.implnet_ops_wadewade27 import harvest_wadewade27

@job
def implnet_job_wadewade27():
    harvest_wadewade27()
