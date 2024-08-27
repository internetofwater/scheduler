from dagster import job

from ops.implnet_ops_nmwdiosenmwdiose2 import harvest_nmwdiosenmwdiose2

@job
def implnet_job_nmwdiosenmwdiose2():
    harvest_nmwdiosenmwdiose2()
