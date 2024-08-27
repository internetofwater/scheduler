from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw9 import implnet_job_nwisgw9

@schedule(cron_schedule="0 20 2 * *", job=implnet_job_nwisgw9, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw9(_context):
    run_config = {}
    return run_config
