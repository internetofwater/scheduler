from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw8 import implnet_job_usgsmonitoringlocationnwisgwnwisgw8

@schedule(cron_schedule="0 2 1 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw8, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw8(_context):
    run_config = {}
    return run_config
