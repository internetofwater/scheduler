from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade20 import implnet_job_wadewade20

@schedule(cron_schedule="0 16 10 * *", job=implnet_job_wadewade20, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade20(_context):
    run_config = {}
    return run_config
