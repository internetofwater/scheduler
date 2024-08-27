from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisdrwiids0 import implnet_job_cuahsihisdrwiids0

@schedule(cron_schedule="0 10 7 * *", job=implnet_job_cuahsihisdrwiids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisdrwiids0(_context):
    run_config = {}
    return run_config
