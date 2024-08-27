from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw27 import harvest_usgsmonitoringlocationnwisgwnwisgw27

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw27():
    harvest_usgsmonitoringlocationnwisgwnwisgw27()
