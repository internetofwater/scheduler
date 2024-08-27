from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_counties0 import implnet_job_counties0

@schedule(cron_schedule="0 12 21 * *", job=implnet_job_counties0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_counties0(_context):
    run_config = {}
    return run_config
