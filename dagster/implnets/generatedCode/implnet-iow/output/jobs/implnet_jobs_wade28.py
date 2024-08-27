from dagster import job

from ops.implnet_ops_wade28 import harvest_wade28

@job
def implnet_job_wade28():
    harvest_wade28()