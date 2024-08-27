from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw27 import implnet_job_usgsmonitoringlocationnwisgwnwisgw27

@schedule(cron_schedule="0 12 1 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw27, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw27(_context):
    run_config = {}
    return run_config
