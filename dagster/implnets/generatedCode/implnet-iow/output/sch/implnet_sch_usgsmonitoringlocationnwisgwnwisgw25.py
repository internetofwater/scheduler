from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw25 import implnet_job_usgsmonitoringlocationnwisgwnwisgw25

@schedule(cron_schedule="0 0 2 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw25, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw25(_context):
    run_config = {}
    return run_config
