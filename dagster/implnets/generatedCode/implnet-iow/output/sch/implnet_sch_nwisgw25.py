from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw25 import implnet_job_nwisgw25

@schedule(cron_schedule="0 16 1 * *", job=implnet_job_nwisgw25, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw25(_context):
    run_config = {}
    return run_config
