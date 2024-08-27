from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade47 import implnet_job_wadewade47

@schedule(cron_schedule="0 8 10 * *", job=implnet_job_wadewade47, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade47(_context):
    run_config = {}
    return run_config
