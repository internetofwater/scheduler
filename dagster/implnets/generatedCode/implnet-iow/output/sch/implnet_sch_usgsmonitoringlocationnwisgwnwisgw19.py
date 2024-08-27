from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw19 import implnet_job_usgsmonitoringlocationnwisgwnwisgw19

@schedule(cron_schedule="0 0 1 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw19, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw19(_context):
    run_config = {}
    return run_config
