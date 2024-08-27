from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw24 import harvest_usgsmonitoringlocationnwisgwnwisgw24

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw24():
    harvest_usgsmonitoringlocationnwisgwnwisgw24()
