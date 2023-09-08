from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_SOURCEVAL import implnet_job_SOURCEVAL

@schedule(cron_schedule="0 24 * * *", job=implnet_job_SOURCEVAL, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_SOURCEVAL(_context):
    run_config = {}
    return run_config
