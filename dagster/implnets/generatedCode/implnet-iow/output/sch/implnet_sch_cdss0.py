from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cdss0 import implnet_job_cdss0

@schedule(cron_schedule="0 14 2 * *", job=implnet_job_cdss0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cdss0(_context):
    run_config = {}
    return run_config
