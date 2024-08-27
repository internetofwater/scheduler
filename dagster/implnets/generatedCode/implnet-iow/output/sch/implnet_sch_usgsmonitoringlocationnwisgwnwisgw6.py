from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw6 import implnet_job_usgsmonitoringlocationnwisgwnwisgw6

@schedule(cron_schedule="0 6 14 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw6, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw6(_context):
    run_config = {}
    return run_config
