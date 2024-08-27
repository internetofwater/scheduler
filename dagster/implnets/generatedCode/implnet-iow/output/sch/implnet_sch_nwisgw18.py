from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw18 import implnet_job_nwisgw18

@schedule(cron_schedule="0 10 1 * *", job=implnet_job_nwisgw18, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw18(_context):
    run_config = {}
    return run_config
