from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade37 import implnet_job_wade37

@schedule(cron_schedule="0 10 11 * *", job=implnet_job_wade37, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade37(_context):
    run_config = {}
    return run_config
