from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw6 import implnet_job_nwisgw6

@schedule(cron_schedule="0 6 14 * *", job=implnet_job_nwisgw6, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw6(_context):
    run_config = {}
    return run_config
