from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_usgsmonitoringlocationnwisgwnwisgw22 import implnet_job_usgsmonitoringlocationnwisgwnwisgw22

@schedule(cron_schedule="0 18 1 * *", job=implnet_job_usgsmonitoringlocationnwisgwnwisgw22, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_usgsmonitoringlocationnwisgwnwisgw22(_context):
    run_config = {}
    return run_config
