from dagster import job

from ops.implnet_ops_wyseo0 import harvest_wyseo0

@job
def implnet_job_wyseo0():
    harvest_wyseo0()