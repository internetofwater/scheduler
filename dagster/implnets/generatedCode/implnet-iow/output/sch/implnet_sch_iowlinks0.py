from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_iowlinks0 import implnet_job_iowlinks0

@schedule(cron_schedule="0 6 2 * *", job=implnet_job_iowlinks0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_iowlinks0(_context):
    run_config = {}
    return run_config
