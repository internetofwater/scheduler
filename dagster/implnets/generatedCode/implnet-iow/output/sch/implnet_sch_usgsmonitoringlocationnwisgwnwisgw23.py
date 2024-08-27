from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw23 import implnet_job_usgsmonitoringlocationnwisgwnwisgw23

@schedule(cron_schedule="0 20 13 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw23, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw23(_context):
    run_config = {}
    return run_config
