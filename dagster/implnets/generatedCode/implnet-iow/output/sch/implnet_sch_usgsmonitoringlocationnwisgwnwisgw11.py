from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw11 import implnet_job_usgsmonitoringlocationnwisgwnwisgw11

@schedule(cron_schedule="0 18 14 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw11, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw11(_context):
    run_config = {}
    return run_config
