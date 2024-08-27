from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw28 import harvest_usgsmonitoringlocationnwisgwnwisgw28

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw28():
    harvest_usgsmonitoringlocationnwisgwnwisgw28()
