from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_damspids0 import implnet_job_damspids0

@schedule(cron_schedule="0 0 7 * *", job=implnet_job_damspids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_damspids0(_context):
    run_config = {}
    return run_config
