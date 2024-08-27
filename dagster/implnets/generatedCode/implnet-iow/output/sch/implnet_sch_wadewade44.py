from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade44 import implnet_job_wadewade44

@schedule(cron_schedule="0 12 9 * *", job=implnet_job_wadewade44, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade44(_context):
    run_config = {}
    return run_config
