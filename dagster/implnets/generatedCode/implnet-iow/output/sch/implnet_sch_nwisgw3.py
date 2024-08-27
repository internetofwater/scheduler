from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw3 import implnet_job_nwisgw3

@schedule(cron_schedule="0 0 4 * *", job=implnet_job_nwisgw3, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw3(_context):
    run_config = {}
    return run_config
