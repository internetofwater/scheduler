from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw5 import harvest_usgsmonitoringlocationnwisgwnwisgw5

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw5():
    harvest_usgsmonitoringlocationnwisgwnwisgw5()
