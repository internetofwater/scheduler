from dagster import job

from ops.implnet_ops_usgshydrologicunit0 import harvest_usgshydrologicunit0

@job
def implnet_job_usgshydrologicunit0():
    harvest_usgshydrologicunit0()
