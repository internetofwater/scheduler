from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw5 import implnet_job_usgsmonitoringlocationnwisgwnwisgw5

@schedule(cron_schedule="0 22 14 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw5, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw5(_context):
    run_config = {}
    return run_config
