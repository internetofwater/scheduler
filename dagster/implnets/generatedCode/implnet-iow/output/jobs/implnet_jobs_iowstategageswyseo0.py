from dagster import job

from ops.implnet_ops_iowstategageswyseo0 import harvest_iowstategageswyseo0

@job
def implnet_job_iowstategageswyseo0():
    harvest_iowstategageswyseo0()
