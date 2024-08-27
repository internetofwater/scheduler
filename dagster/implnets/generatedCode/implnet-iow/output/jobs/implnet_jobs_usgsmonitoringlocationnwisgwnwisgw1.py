from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw1 import harvest_usgsmonitoringlocationnwisgwnwisgw1

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw1():
    harvest_usgsmonitoringlocationnwisgwnwisgw1()
