from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade2 import implnet_job_wade2

@schedule(cron_schedule="0 20 27 * *", job=implnet_job_wade2, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade2(_context):
    run_config = {}
    return run_config
