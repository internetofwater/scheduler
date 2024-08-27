from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw2 import implnet_job_nwisgw2

@schedule(cron_schedule="0 8 14 * *", job=implnet_job_nwisgw2, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw2(_context):
    run_config = {}
    return run_config
