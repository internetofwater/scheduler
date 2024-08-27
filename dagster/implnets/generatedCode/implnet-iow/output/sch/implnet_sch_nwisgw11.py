from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw11 import implnet_job_nwisgw11

@schedule(cron_schedule="0 12 5 * *", job=implnet_job_nwisgw11, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw11(_context):
    run_config = {}
    return run_config
