from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade17 import implnet_job_wadewade17

@schedule(cron_schedule="0 2 9 * *", job=implnet_job_wadewade17, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade17(_context):
    run_config = {}
    return run_config
