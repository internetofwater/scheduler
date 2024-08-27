from dagster import job

from ops.implnet_ops_wade49 import harvest_wade49

@job
def implnet_job_wade49():
    harvest_wade49()