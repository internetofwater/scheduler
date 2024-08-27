from dagster import job

from ops.implnet_ops_wade44 import harvest_wade44

@job
def implnet_job_wade44():
    harvest_wade44()