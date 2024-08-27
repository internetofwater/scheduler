from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw7 import harvest_usgsmonitoringlocationnwisgwnwisgw7

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw7():
    harvest_usgsmonitoringlocationnwisgwnwisgw7()
