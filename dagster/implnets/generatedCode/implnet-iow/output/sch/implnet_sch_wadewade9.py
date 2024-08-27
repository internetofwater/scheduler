from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade9 import implnet_job_wadewade9

@schedule(cron_schedule="0 0 12 * *", job=implnet_job_wadewade9, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade9(_context):
    run_config = {}
    return run_config
