from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade45 import implnet_job_wade45

@schedule(cron_schedule="0 12 12 * *", job=implnet_job_wade45, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade45(_context):
    run_config = {}
    return run_config
