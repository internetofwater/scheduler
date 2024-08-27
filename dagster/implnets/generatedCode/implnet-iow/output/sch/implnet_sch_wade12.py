from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade12 import implnet_job_wade12

@schedule(cron_schedule="0 4 3 * *", job=implnet_job_wade12, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade12(_context):
    run_config = {}
    return run_config
