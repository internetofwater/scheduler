from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw18 import implnet_job_usgsmonitoringlocationnwisgwnwisgw18

@schedule(cron_schedule="0 10 1 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw18, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw18(_context):
    run_config = {}
    return run_config
