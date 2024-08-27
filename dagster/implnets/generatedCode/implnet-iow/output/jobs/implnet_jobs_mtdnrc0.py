from dagster import job

from ops.implnet_ops_mtdnrc0 import harvest_mtdnrc0

@job
def implnet_job_mtdnrc0():
    harvest_mtdnrc0()