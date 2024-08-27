from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw3 import harvest_usgsmonitoringlocationnwisgwnwisgw3

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw3():
    harvest_usgsmonitoringlocationnwisgwnwisgw3()
