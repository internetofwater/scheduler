from dagster import job

from ops.implnet_ops_wade40 import harvest_wade40

@job
def implnet_job_wade40():
    harvest_wade40()