from dagster import job

from ops.implnet_ops_cdss0 import harvest_cdss0

@job
def implnet_job_cdss0():
    harvest_cdss0()