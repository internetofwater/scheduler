from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw23 import harvest_usgsmonitoringlocationnwisgwnwisgw23

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw23():
    harvest_usgsmonitoringlocationnwisgwnwisgw23()
