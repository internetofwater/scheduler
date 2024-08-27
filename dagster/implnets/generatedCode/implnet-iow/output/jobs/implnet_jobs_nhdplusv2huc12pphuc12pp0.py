from dagster import job

from ops.implnet_ops_nhdplusv2huc12pphuc12pp0 import harvest_nhdplusv2huc12pphuc12pp0

@job
def implnet_job_nhdplusv2huc12pphuc12pp0():
    harvest_nhdplusv2huc12pphuc12pp0()
