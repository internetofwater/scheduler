from dagster import job

from ops.implnet_ops_refgagesgages3 import harvest_refgagesgages3

@job
def implnet_job_refgagesgages3():
    harvest_refgagesgages3()
