from dagster import job

from ops.implnet_ops_wade45 import harvest_wade45

@job
def implnet_job_wade45():
    harvest_wade45()