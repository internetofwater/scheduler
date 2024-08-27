from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade4 import implnet_job_wade4

@schedule(cron_schedule="0 8 3 * *", job=implnet_job_wade4, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade4(_context):
    run_config = {}
    return run_config
