from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade27 import implnet_job_wadewade27

@schedule(cron_schedule="0 14 11 * *", job=implnet_job_wadewade27, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade27(_context):
    run_config = {}
    return run_config
