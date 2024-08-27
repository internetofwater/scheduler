from dagster import job

from ops.implnet_ops_iowstategagesndwr0 import harvest_iowstategagesndwr0

@job
def implnet_job_iowstategagesndwr0():
    harvest_iowstategagesndwr0()
