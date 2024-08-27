from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_wadewade39 import implnet_job_wadewade39

@schedule(cron_schedule="0 8 11 * *", job=implnet_job_wadewade39, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_wadewade39(_context):
    run_config = {}
    return run_config
