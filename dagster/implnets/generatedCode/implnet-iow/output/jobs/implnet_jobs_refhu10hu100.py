from dagster import job

from ops.implnet_ops_refhu10hu100 import harvest_refhu10hu100

@job
def implnet_job_refhu10hu100():
    harvest_refhu10hu100()
