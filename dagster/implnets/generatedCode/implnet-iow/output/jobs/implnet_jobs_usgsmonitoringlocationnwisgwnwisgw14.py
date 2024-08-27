from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw14 import harvest_usgsmonitoringlocationnwisgwnwisgw14

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw14():
    harvest_usgsmonitoringlocationnwisgwnwisgw14()
