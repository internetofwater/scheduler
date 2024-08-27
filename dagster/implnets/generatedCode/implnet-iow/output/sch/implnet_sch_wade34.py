from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade34 import implnet_job_wade34

@schedule(cron_schedule="0 14 12 * *", job=implnet_job_wade34, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade34(_context):
    run_config = {}
    return run_config
