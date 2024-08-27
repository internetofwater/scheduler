from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisyosemitehydroclimatenetworkids0 import implnet_job_cuahsihisyosemitehydroclimatenetworkids0

@schedule(cron_schedule="0 18 1 * *", job=implnet_job_cuahsihisyosemitehydroclimatenetworkids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisyosemitehydroclimatenetworkids0(_context):
    run_config = {}
    return run_config
