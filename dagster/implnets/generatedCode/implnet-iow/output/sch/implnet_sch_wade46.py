from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade46 import implnet_job_wade46

@schedule(cron_schedule="0 22 10 * *", job=implnet_job_wade46, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade46(_context):
    run_config = {}
    return run_config
