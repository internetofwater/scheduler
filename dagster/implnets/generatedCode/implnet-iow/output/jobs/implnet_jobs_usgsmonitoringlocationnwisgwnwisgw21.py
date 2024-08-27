from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw21 import harvest_usgsmonitoringlocationnwisgwnwisgw21

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw21():
    harvest_usgsmonitoringlocationnwisgwnwisgw21()
