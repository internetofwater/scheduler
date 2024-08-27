from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw5 import implnet_job_nwisgw5

@schedule(cron_schedule="0 12 3 * *", job=implnet_job_nwisgw5, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw5(_context):
    run_config = {}
    return run_config
