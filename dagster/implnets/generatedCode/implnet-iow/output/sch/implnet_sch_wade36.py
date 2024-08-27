from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade36 import implnet_job_wade36

@schedule(cron_schedule="0 0 11 * *", job=implnet_job_wade36, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade36(_context):
    run_config = {}
    return run_config
