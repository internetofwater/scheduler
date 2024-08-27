from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw20 import harvest_usgsmonitoringlocationnwisgwnwisgw20

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw20():
    harvest_usgsmonitoringlocationnwisgwnwisgw20()
