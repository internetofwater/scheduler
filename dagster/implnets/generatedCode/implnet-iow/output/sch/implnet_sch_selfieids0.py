from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_selfieids0 import implnet_job_selfieids0

@schedule(cron_schedule="0 16 26 * *", job=implnet_job_selfieids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_selfieids0(_context):
    run_config = {}
    return run_config
