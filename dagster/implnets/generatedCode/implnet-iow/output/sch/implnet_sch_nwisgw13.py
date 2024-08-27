from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw13 import implnet_job_nwisgw13

@schedule(cron_schedule="0 16 14 * *", job=implnet_job_nwisgw13, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw13(_context):
    run_config = {}
    return run_config
