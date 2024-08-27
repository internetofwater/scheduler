from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade32 import implnet_job_wade32

@schedule(cron_schedule="0 4 9 * *", job=implnet_job_wade32, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade32(_context):
    run_config = {}
    return run_config
