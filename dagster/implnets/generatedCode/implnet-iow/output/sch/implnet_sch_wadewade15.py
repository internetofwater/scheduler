from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade15 import implnet_job_wadewade15

@schedule(cron_schedule="0 6 9 * *", job=implnet_job_wadewade15, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade15(_context):
    run_config = {}
    return run_config
