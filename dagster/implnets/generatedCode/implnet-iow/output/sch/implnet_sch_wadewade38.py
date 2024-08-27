from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade38 import implnet_job_wadewade38

@schedule(cron_schedule="0 16 12 * *", job=implnet_job_wadewade38, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade38(_context):
    run_config = {}
    return run_config
