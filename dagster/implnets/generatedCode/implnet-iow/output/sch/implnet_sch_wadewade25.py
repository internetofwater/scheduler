from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade25 import implnet_job_wadewade25

@schedule(cron_schedule="0 16 9 * *", job=implnet_job_wadewade25, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade25(_context):
    run_config = {}
    return run_config
