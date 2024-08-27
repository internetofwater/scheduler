from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw16 import implnet_job_usgsmonitoringlocationnwisgwnwisgw16

@schedule(cron_schedule="0 4 14 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw16, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw16(_context):
    run_config = {}
    return run_config
