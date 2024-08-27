from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_selfieselfieids0 import implnet_job_selfieselfieids0

@schedule(cron_schedule="0 2 13 * *", job=implnet_job_selfieselfieids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_selfieselfieids0(_context):
    run_config = {}
    return run_config
