from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade41 import implnet_job_wadewade41

@schedule(cron_schedule="0 18 8 * *", job=implnet_job_wadewade41, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade41(_context):
    run_config = {}
    return run_config
