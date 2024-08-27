from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw10 import harvest_usgsmonitoringlocationnwisgwnwisgw10

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw10():
    harvest_usgsmonitoringlocationnwisgwnwisgw10()
