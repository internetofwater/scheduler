from dagster import job

from ops.implnet_ops_nhdplusv2huc12pphuc12pp1 import harvest_nhdplusv2huc12pphuc12pp1

@job
def implnet_job_nhdplusv2huc12pphuc12pp1():
    harvest_nhdplusv2huc12pphuc12pp1()
