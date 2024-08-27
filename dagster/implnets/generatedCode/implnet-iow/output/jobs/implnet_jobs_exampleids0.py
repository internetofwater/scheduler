from dagster import job

from ops.implnet_ops_exampleids0 import harvest_exampleids0

@job
def implnet_job_exampleids0():
    harvest_exampleids0()