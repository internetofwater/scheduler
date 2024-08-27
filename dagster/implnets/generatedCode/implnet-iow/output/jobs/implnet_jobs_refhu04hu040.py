from dagster import job

from ops.implnet_ops_refhu04hu040 import harvest_refhu04hu040

@job
def implnet_job_refhu04hu040():
    harvest_refhu04hu040()
