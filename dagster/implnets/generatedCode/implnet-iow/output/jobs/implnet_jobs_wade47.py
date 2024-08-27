from dagster import job

from ops.implnet_ops_wade47 import harvest_wade47

@job
def implnet_job_wade47():
    harvest_wade47()