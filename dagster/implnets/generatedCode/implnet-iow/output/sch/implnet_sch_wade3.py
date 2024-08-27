from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade3 import implnet_job_wade3

@schedule(cron_schedule="0 16 1 * *", job=implnet_job_wade3, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade3(_context):
    run_config = {}
    return run_config
