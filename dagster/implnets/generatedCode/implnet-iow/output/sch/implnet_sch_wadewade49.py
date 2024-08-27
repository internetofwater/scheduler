from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade49 import implnet_job_wadewade49

@schedule(cron_schedule="0 4 12 * *", job=implnet_job_wadewade49, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade49(_context):
    run_config = {}
    return run_config
