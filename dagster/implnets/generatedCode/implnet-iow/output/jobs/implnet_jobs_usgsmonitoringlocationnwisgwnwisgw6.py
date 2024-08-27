from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw6 import harvest_usgsmonitoringlocationnwisgwnwisgw6

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw6():
    harvest_usgsmonitoringlocationnwisgwnwisgw6()
