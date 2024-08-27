from dagster import job

from ops.implnet_ops_wade26 import harvest_wade26

@job
def implnet_job_wade26():
    harvest_wade26()