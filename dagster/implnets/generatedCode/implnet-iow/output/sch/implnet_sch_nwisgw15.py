from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw15 import implnet_job_nwisgw15

@schedule(cron_schedule="0 2 2 * *", job=implnet_job_nwisgw15, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw15(_context):
    run_config = {}
    return run_config
