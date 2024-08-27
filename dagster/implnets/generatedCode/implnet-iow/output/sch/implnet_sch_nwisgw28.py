from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_nwisgw28 import implnet_job_nwisgw28

@schedule(cron_schedule="0 4 3 * *", job=implnet_job_nwisgw28, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_nwisgw28(_context):
    run_config = {}
    return run_config
