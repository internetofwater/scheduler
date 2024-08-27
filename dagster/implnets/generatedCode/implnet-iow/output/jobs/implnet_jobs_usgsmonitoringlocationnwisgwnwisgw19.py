from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw19 import harvest_usgsmonitoringlocationnwisgwnwisgw19

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw19():
    harvest_usgsmonitoringlocationnwisgwnwisgw19()
