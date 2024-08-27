from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw21 import implnet_job_nwisgw21

@schedule(cron_schedule="0 22 13 * *", job=implnet_job_nwisgw21, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw21(_context):
    run_config = {}
    return run_config
