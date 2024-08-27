from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw20 import implnet_job_nwisgw20

@schedule(cron_schedule="0 4 1 * *", job=implnet_job_nwisgw20, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw20(_context):
    run_config = {}
    return run_config
