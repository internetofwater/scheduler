from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade8 import implnet_job_wade8

@schedule(cron_schedule="0 18 9 * *", job=implnet_job_wade8, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade8(_context):
    run_config = {}
    return run_config
