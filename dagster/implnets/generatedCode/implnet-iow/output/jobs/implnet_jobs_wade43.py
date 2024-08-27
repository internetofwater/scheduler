from dagster import job

from ops.implnet_ops_wade43 import harvest_wade43

@job
def implnet_job_wade43():
    harvest_wade43()