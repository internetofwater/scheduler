from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw22 import harvest_usgsmonitoringlocationnwisgwnwisgw22

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw22():
    harvest_usgsmonitoringlocationnwisgwnwisgw22()
