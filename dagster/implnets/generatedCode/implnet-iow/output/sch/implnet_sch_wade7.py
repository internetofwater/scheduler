from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade7 import implnet_job_wade7

@schedule(cron_schedule="0 12 1 * *", job=implnet_job_wade7, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade7(_context):
    run_config = {}
    return run_config
