from dagster import job

from ops.implnet_ops_refdamsdams0 import harvest_refdamsdams0

@job
def implnet_job_refdamsdams0():
    harvest_refdamsdams0()
