from dagster import job

from ops.implnet_ops_iowusgssta0 import harvest_iowusgssta0

@job
def implnet_job_iowusgssta0():
    harvest_iowusgssta0()
