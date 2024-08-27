from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw21 import implnet_job_usgsmonitoringlocationnwisgwnwisgw21

@schedule(cron_schedule="0 22 13 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw21, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw21(_context):
    run_config = {}
    return run_config
