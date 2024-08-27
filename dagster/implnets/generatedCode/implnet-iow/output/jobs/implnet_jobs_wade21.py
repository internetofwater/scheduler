from dagster import job

from ops.implnet_ops_wade21 import harvest_wade21

@job
def implnet_job_wade21():
    harvest_wade21()