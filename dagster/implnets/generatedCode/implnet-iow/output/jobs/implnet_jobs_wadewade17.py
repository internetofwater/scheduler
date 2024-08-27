from dagster import job

from ops.implnet_ops_wadewade17 import harvest_wadewade17

@job
def implnet_job_wadewade17():
    harvest_wadewade17()
