from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw24 import implnet_job_usgsmonitoringlocationnwisgwnwisgw24

@schedule(cron_schedule="0 16 1 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw24, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw24(_context):
    run_config = {}
    return run_config
