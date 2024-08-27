from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw11 import harvest_usgsmonitoringlocationnwisgwnwisgw11

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw11():
    harvest_usgsmonitoringlocationnwisgwnwisgw11()
