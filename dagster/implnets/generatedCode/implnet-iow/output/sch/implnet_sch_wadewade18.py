from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade18 import implnet_job_wadewade18

@schedule(cron_schedule="0 6 12 * *", job=implnet_job_wadewade18, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade18(_context):
    run_config = {}
    return run_config
