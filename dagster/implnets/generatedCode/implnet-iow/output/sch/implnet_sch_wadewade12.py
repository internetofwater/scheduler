from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade12 import implnet_job_wadewade12

@schedule(cron_schedule="0 6 11 * *", job=implnet_job_wadewade12, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade12(_context):
    run_config = {}
    return run_config
