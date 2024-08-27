from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw7 import implnet_job_usgsmonitoringlocationnwisgwnwisgw7

@schedule(cron_schedule="0 22 1 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw7, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw7(_context):
    run_config = {}
    return run_config
