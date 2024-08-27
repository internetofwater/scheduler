from dagster import job

from ops.implnet_ops_refdamsdams1 import harvest_refdamsdams1

@job
def implnet_job_refdamsdams1():
    harvest_refdamsdams1()
