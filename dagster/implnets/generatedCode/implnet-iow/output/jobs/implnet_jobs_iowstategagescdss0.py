from dagster import job

from ops.implnet_ops_iowstategagescdss0 import harvest_iowstategagescdss0

@job
def implnet_job_iowstategagescdss0():
    harvest_iowstategagescdss0()
