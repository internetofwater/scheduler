from dagster import job

from ops.implnet_ops_iowstategagesmtdnrc0 import harvest_iowstategagesmtdnrc0

@job
def implnet_job_iowstategagesmtdnrc0():
    harvest_iowstategagesmtdnrc0()
