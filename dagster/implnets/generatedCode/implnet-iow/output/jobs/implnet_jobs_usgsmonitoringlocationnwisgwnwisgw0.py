from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw0 import harvest_usgsmonitoringlocationnwisgwnwisgw0

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw0():
    harvest_usgsmonitoringlocationnwisgwnwisgw0()
