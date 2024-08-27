from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade11 import implnet_job_wadewade11

@schedule(cron_schedule="0 16 11 * *", job=implnet_job_wadewade11, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade11(_context):
    run_config = {}
    return run_config
