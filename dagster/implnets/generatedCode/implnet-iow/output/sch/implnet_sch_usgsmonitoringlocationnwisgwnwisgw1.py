from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw1 import implnet_job_usgsmonitoringlocationnwisgwnwisgw1

@schedule(cron_schedule="0 6 1 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw1, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw1(_context):
    run_config = {}
    return run_config
