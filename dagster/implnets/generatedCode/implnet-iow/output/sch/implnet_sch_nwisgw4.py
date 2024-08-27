from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw4 import implnet_job_nwisgw4

@schedule(cron_schedule="0 12 14 * *", job=implnet_job_nwisgw4, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw4(_context):
    run_config = {}
    return run_config
