from dagster import job

from ops.implnet_ops_refhu06hu060 import harvest_refhu06hu060

@job
def implnet_job_refhu06hu060():
    harvest_refhu06hu060()
