from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_refhu02hu020 import implnet_job_refhu02hu020

@schedule(cron_schedule="0 18 3 * *", job=implnet_job_refhu02hu020, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_refhu02hu020(_context):
    run_config = {}
    return run_config
