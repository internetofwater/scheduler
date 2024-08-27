from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw17 import harvest_usgsmonitoringlocationnwisgwnwisgw17

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw17():
    harvest_usgsmonitoringlocationnwisgwnwisgw17()
