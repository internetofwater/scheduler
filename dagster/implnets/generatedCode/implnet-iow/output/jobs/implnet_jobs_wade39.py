from dagster import job

from ops.implnet_ops_wade39 import harvest_wade39

@job
def implnet_job_wade39():
    harvest_wade39()