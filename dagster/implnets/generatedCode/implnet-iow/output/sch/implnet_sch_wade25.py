from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade25 import implnet_job_wade25

@schedule(cron_schedule="0 16 9 * *", job=implnet_job_wade25, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade25(_context):
    run_config = {}
    return run_config
