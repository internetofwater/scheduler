from dagster import job

from ops.implnet_ops_nmwdiosenmwdiose0 import harvest_nmwdiosenmwdiose0

@job
def implnet_job_nmwdiosenmwdiose0():
    harvest_nmwdiosenmwdiose0()
