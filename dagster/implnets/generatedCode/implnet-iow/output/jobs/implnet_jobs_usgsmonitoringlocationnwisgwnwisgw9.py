from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw9 import harvest_usgsmonitoringlocationnwisgwnwisgw9

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw9():
    harvest_usgsmonitoringlocationnwisgwnwisgw9()
