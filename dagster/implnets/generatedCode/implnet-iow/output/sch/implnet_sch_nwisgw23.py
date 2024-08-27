from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw23 import implnet_job_nwisgw23

@schedule(cron_schedule="0 20 13 * *", job=implnet_job_nwisgw23, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw23(_context):
    run_config = {}
    return run_config
