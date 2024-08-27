from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_ua100 import implnet_job_ua100

@schedule(cron_schedule="0 4 4 * *", job=implnet_job_ua100, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_ua100(_context):
    run_config = {}
    return run_config
