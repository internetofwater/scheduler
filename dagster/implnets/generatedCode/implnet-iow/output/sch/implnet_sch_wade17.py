from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade17 import implnet_job_wade17

@schedule(cron_schedule="0 2 9 * *", job=implnet_job_wade17, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade17(_context):
    run_config = {}
    return run_config
