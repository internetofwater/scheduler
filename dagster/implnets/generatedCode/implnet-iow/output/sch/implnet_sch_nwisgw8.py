from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw8 import implnet_job_nwisgw8

@schedule(cron_schedule="0 20 4 * *", job=implnet_job_nwisgw8, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw8(_context):
    run_config = {}
    return run_config
