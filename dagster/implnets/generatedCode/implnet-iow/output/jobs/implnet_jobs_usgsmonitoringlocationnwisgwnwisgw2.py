from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw2 import harvest_usgsmonitoringlocationnwisgwnwisgw2

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw2():
    harvest_usgsmonitoringlocationnwisgwnwisgw2()
