from dagster import job

from ops.implnet_ops_usgsmonitoringlocationnwisgwnwisgw26 import harvest_usgsmonitoringlocationnwisgwnwisgw26

@job
def implnet_job_usgsmonitoringlocationnwisgwnwisgw26():
    harvest_usgsmonitoringlocationnwisgwnwisgw26()
