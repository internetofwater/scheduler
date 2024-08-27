from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw13 import harvest_usgsmonitoringlocationnwisgwnwisgw13

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw13():
    harvest_usgsmonitoringlocationnwisgwnwisgw13()
