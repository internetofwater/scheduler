from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade16 import implnet_job_wadewade16

@schedule(cron_schedule="0 8 9 * *", job=implnet_job_wadewade16, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade16(_context):
    run_config = {}
    return run_config
