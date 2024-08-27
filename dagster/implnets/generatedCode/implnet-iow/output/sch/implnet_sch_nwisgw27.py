from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw27 import implnet_job_nwisgw27

@schedule(cron_schedule="0 16 4 * *", job=implnet_job_nwisgw27, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw27(_context):
    run_config = {}
    return run_config
