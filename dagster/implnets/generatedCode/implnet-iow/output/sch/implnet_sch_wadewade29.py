from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade29 import implnet_job_wadewade29

@schedule(cron_schedule="0 18 11 * *", job=implnet_job_wadewade29, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade29(_context):
    run_config = {}
    return run_config
