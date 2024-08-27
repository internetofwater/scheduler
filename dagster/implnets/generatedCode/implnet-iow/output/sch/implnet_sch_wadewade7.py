from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade7 import implnet_job_wadewade7

@schedule(cron_schedule="0 0 9 * *", job=implnet_job_wadewade7, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade7(_context):
    run_config = {}
    return run_config
