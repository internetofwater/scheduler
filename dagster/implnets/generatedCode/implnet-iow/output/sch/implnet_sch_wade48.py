from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade48 import implnet_job_wade48

@schedule(cron_schedule="0 14 9 * *", job=implnet_job_wade48, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade48(_context):
    run_config = {}
    return run_config
