from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade13 import implnet_job_wadewade13

@schedule(cron_schedule="0 22 8 * *", job=implnet_job_wadewade13, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade13(_context):
    run_config = {}
    return run_config
