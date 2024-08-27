from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisyosemitehydroclimatenetworkids0 import implnet_job_cuahsicuahsihisyosemitehydroclimatenetworkids0

@schedule(cron_schedule="0 18 1 * *", job=implnet_job_cuahsicuahsihisyosemitehydroclimatenetworkids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisyosemitehydroclimatenetworkids0(_context):
    run_config = {}
    return run_config
