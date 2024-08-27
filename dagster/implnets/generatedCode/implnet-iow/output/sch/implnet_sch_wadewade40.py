from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade40 import implnet_job_wadewade40

@schedule(cron_schedule="0 4 10 * *", job=implnet_job_wadewade40, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade40(_context):
    run_config = {}
    return run_config
