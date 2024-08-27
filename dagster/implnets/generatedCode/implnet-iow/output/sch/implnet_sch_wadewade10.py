from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade10 import implnet_job_wadewade10

@schedule(cron_schedule="0 12 11 * *", job=implnet_job_wadewade10, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade10(_context):
    run_config = {}
    return run_config
