from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade11 import implnet_job_wade11

@schedule(cron_schedule="0 0 4 * *", job=implnet_job_wade11, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade11(_context):
    run_config = {}
    return run_config
