from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade9 import implnet_job_wade9

@schedule(cron_schedule="0 0 12 * *", job=implnet_job_wade9, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade9(_context):
    run_config = {}
    return run_config
