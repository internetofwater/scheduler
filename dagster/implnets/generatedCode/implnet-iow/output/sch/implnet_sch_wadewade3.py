from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade3 import implnet_job_wadewade3

@schedule(cron_schedule="0 18 12 * *", job=implnet_job_wadewade3, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade3(_context):
    run_config = {}
    return run_config
