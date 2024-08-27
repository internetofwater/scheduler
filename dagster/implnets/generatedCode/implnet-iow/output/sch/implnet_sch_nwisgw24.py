from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw24 import implnet_job_nwisgw24

@schedule(cron_schedule="0 16 1 * *", job=implnet_job_nwisgw24, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw24(_context):
    run_config = {}
    return run_config
