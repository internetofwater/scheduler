from dagster import job

from ops.implnet_ops_refgagesgages1 import harvest_refgagesgages1

@job
def implnet_job_refgagesgages1():
    harvest_refgagesgages1()
