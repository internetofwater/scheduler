from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_princiaq0 import implnet_job_princiaq0

@schedule(cron_schedule="0 12 3 * *", job=implnet_job_princiaq0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_princiaq0(_context):
    run_config = {}
    return run_config
