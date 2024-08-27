from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw9 import implnet_job_usgsmonitoringlocationnwisgwnwisgw9

@schedule(cron_schedule="0 0 14 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw9, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw9(_context):
    run_config = {}
    return run_config
