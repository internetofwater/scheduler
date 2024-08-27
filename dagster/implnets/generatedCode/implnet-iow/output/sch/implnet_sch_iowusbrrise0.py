from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_iowusbrrise0 import implnet_job_iowusbrrise0

@schedule(cron_schedule="0 20 2 * *", job=implnet_job_iowusbrrise0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_iowusbrrise0(_context):
    run_config = {}
    return run_config
