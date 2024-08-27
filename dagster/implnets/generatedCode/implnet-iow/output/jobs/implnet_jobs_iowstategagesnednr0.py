from dagster import job

from ops.implnet_ops_iowstategagesnednr0 import harvest_iowstategagesnednr0

@job
def implnet_job_iowstategagesnednr0():
    harvest_iowstategagesnednr0()
