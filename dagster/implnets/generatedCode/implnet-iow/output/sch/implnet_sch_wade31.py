from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wade31 import implnet_job_wade31

@schedule(cron_schedule="0 4 11 * *", job=implnet_job_wade31, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wade31(_context):
    run_config = {}
    return run_config
