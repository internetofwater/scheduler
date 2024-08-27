from dagster import job

from ops.implnet_ops_refpwspws0 import harvest_refpwspws0

@job
def implnet_job_refpwspws0():
    harvest_refpwspws0()
