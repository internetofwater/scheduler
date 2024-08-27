from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw12 import implnet_job_usgsmonitoringlocationnwisgwnwisgw12

@schedule(cron_schedule="0 20 1 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw12, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw12(_context):
    run_config = {}
    return run_config
