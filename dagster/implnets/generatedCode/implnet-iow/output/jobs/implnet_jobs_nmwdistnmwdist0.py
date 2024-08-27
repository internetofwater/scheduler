from dagster import job

from ops.implnet_ops_nmwdistnmwdist0 import harvest_nmwdistnmwdist0

@job
def implnet_job_nmwdistnmwdist0():
    harvest_nmwdistnmwdist0()
