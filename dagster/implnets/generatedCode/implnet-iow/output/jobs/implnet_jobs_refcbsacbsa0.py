from dagster import job

from ops.implnet_ops_refcbsacbsa0 import harvest_refcbsacbsa0

@job
def implnet_job_refcbsacbsa0():
    harvest_refcbsacbsa0()
