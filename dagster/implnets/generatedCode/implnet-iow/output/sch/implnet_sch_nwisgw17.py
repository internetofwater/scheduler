from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw17 import implnet_job_nwisgw17

@schedule(cron_schedule="0 0 5 * *", job=implnet_job_nwisgw17, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw17(_context):
    run_config = {}
    return run_config
