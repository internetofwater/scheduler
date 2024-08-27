from dagster import job

from ops.implnet_ops_epahmwhmw0 import harvest_epahmwhmw0

@job
def implnet_job_epahmwhmw0():
    harvest_epahmwhmw0()
