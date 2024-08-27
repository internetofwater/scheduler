from dagster import job

from ops.implnet_ops_refcountiescounties0 import harvest_refcountiescounties0

@job
def implnet_job_refcountiescounties0():
    harvest_refcountiescounties0()
