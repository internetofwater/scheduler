from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw8 import harvest_usgsmonitoringlocationnwisgwnwisgw8

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw8():
    harvest_usgsmonitoringlocationnwisgwnwisgw8()
