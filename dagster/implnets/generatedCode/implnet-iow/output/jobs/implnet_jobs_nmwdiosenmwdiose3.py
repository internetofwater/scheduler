from dagster import job

from ops.implnet_ops_nmwdiosenmwdiose3 import harvest_nmwdiosenmwdiose3

@job
def implnet_job_nmwdiosenmwdiose3():
    harvest_nmwdiosenmwdiose3()
