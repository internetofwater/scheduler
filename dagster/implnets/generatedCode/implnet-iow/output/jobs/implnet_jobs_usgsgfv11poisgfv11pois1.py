from dagster import job

from ops.implnet_ops_usgsgfv11poisgfv11pois1 import harvest_usgsgfv11poisgfv11pois1

@job
def implnet_job_usgsgfv11poisgfv11pois1():
    harvest_usgsgfv11poisgfv11pois1()
