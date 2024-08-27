from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade23 import implnet_job_wadewade23

@schedule(cron_schedule="0 20 10 * *", job=implnet_job_wadewade23, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade23(_context):
    run_config = {}
    return run_config
