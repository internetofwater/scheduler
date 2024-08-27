from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade23 import implnet_job_wade23

@schedule(cron_schedule="0 20 10 * *", job=implnet_job_wade23, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade23(_context):
    run_config = {}
    return run_config
