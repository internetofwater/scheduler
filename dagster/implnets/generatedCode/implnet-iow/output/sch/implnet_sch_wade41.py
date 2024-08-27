from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade41 import implnet_job_wade41

@schedule(cron_schedule="0 18 8 * *", job=implnet_job_wade41, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade41(_context):
    run_config = {}
    return run_config
