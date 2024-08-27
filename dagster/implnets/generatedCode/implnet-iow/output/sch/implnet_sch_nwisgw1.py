from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw1 import implnet_job_nwisgw1

@schedule(cron_schedule="0 6 1 * *", job=implnet_job_nwisgw1, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw1(_context):
    run_config = {}
    return run_config
