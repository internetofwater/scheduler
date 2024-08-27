from dagster import job

from ops.implnet_ops_refhu08hu080 import harvest_refhu08hu080

@job
def implnet_job_refhu08hu080():
    harvest_refhu08hu080()
