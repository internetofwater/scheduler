from dagster import job

from ops.implnet_ops_refgagesgages2 import harvest_refgagesgages2

@job
def implnet_job_refgagesgages2():
    harvest_refgagesgages2()
