from dagster import job

from ops.implnet_ops_nmwdiosenmwdiose4 import harvest_nmwdiosenmwdiose4

@job
def implnet_job_nmwdiosenmwdiose4():
    harvest_nmwdiosenmwdiose4()
