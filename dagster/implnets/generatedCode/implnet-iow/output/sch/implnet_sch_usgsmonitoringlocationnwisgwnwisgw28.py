from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw28 import implnet_job_usgsmonitoringlocationnwisgwnwisgw28

@schedule(cron_schedule="0 10 14 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw28, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw28(_context):
    run_config = {}
    return run_config
