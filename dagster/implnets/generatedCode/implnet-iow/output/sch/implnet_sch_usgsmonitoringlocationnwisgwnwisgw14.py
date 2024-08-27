from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw14 import implnet_job_usgsmonitoringlocationnwisgwnwisgw14

@schedule(cron_schedule="0 20 14 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw14, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw14(_context):
    run_config = {}
    return run_config
