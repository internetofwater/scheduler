from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_places0 import implnet_job_places0

@schedule(cron_schedule="0 14 3 * *", job=implnet_job_places0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_places0(_context):
    run_config = {}
    return run_config
