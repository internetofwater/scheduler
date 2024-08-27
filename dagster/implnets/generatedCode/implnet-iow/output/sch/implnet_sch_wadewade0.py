from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade0 import implnet_job_wadewade0

@schedule(cron_schedule="0 20 8 * *", job=implnet_job_wadewade0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade0(_context):
    run_config = {}
    return run_config
