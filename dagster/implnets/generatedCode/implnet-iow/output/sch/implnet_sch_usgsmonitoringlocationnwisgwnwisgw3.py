from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw3 import implnet_job_usgsmonitoringlocationnwisgwnwisgw3

@schedule(cron_schedule="0 14 1 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw3, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw3(_context):
    run_config = {}
    return run_config
