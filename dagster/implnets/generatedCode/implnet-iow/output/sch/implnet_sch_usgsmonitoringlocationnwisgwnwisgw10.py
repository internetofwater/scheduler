from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw10 import implnet_job_usgsmonitoringlocationnwisgwnwisgw10

@schedule(cron_schedule="0 18 13 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw10, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw10(_context):
    run_config = {}
    return run_config
