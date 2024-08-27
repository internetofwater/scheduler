from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade24 import implnet_job_wadewade24

@schedule(cron_schedule="0 10 10 * *", job=implnet_job_wadewade24, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade24(_context):
    run_config = {}
    return run_config
