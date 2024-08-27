from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade6 import implnet_job_wadewade6

@schedule(cron_schedule="0 10 12 * *", job=implnet_job_wadewade6, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade6(_context):
    run_config = {}
    return run_config
