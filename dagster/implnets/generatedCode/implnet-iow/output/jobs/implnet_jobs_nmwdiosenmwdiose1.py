from dagster import job

from ops.implnet_ops_nmwdiosenmwdiose1 import harvest_nmwdiosenmwdiose1

@job
def implnet_job_nmwdiosenmwdiose1():
    harvest_nmwdiosenmwdiose1()
