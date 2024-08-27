from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw17 import implnet_job_usgsmonitoringlocationnwisgwnwisgw17

@schedule(cron_schedule="0 2 14 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw17, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw17(_context):
    run_config = {}
    return run_config
