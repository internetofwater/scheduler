from dagster import job

from ops.implnet_ops_refhu02hu020 import harvest_refhu02hu020

@job
def implnet_job_refhu02hu020():
    harvest_refhu02hu020()
