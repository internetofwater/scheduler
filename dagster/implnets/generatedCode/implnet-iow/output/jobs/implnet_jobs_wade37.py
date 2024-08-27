from dagster import job

from ops.implnet_ops_wade37 import harvest_wade37

@job
def implnet_job_wade37():
    harvest_wade37()