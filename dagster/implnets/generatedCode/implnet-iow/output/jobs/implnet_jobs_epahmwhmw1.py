from dagster import job

from ops.implnet_ops_epahmwhmw1 import harvest_epahmwhmw1

@job
def implnet_job_epahmwhmw1():
    harvest_epahmwhmw1()
