from dagster import job

from ops.implnet_ops_refua10ua100 import harvest_refua10ua100

@job
def implnet_job_refua10ua100():
    harvest_refua10ua100()
