from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw4 import harvest_usgsmonitoringlocationnwisgwnwisgw4

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw4():
    harvest_usgsmonitoringlocationnwisgwnwisgw4()
