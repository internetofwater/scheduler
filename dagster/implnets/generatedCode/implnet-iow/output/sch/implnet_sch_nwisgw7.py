from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw7 import implnet_job_nwisgw7

@schedule(cron_schedule="0 8 5 * *", job=implnet_job_nwisgw7, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw7(_context):
    run_config = {}
    return run_config
