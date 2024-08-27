from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw15 import harvest_usgsmonitoringlocationnwisgwnwisgw15

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw15():
    harvest_usgsmonitoringlocationnwisgwnwisgw15()
