from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw25 import harvest_usgsmonitoringlocationnwisgwnwisgw25

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw25():
    harvest_usgsmonitoringlocationnwisgwnwisgw25()
