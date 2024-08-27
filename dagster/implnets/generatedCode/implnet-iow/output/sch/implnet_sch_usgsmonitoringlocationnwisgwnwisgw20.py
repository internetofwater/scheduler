from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw20 import implnet_job_usgsmonitoringlocationnwisgwnwisgw20

@schedule(cron_schedule="0 4 1 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw20, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw20(_context):
    run_config = {}
    return run_config
