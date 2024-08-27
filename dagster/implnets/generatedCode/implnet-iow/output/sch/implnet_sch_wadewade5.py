from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade5 import implnet_job_wadewade5

@schedule(cron_schedule="0 20 11 * *", job=implnet_job_wadewade5, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade5(_context):
    run_config = {}
    return run_config
