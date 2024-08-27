from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade24 import implnet_job_wade24

@schedule(cron_schedule="0 10 10 * *", job=implnet_job_wade24, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade24(_context):
    run_config = {}
    return run_config
