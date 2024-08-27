from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw4 import implnet_job_usgsmonitoringlocationnwisgwnwisgw4

@schedule(cron_schedule="0 12 14 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw4, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw4(_context):
    run_config = {}
    return run_config
