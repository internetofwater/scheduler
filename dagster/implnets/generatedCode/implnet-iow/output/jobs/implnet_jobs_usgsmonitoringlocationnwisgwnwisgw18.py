from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw18 import harvest_usgsmonitoringlocationnwisgwnwisgw18

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw18():
    harvest_usgsmonitoringlocationnwisgwnwisgw18()
