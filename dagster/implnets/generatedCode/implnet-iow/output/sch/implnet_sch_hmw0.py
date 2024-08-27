from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_hmw0 import implnet_job_hmw0

@schedule(cron_schedule="0 0 25 * *", job=implnet_job_hmw0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_hmw0(_context):
    run_config = {}
    return run_config
