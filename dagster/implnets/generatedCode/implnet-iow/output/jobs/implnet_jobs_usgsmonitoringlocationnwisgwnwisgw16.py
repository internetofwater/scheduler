from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw16 import harvest_usgsmonitoringlocationnwisgwnwisgw16

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw16():
    harvest_usgsmonitoringlocationnwisgwnwisgw16()
