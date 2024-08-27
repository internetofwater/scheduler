from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw12 import harvest_usgsmonitoringlocationnwisgwnwisgw12

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw12():
    harvest_usgsmonitoringlocationnwisgwnwisgw12()
