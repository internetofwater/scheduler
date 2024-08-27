from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisnooksackmicroclimatenetworkids0 import implnet_job_cuahsicuahsihisnooksackmicroclimatenetworkids0

@schedule(cron_schedule="0 2 5 * *", job=implnet_job_cuahsicuahsihisnooksackmicroclimatenetworkids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisnooksackmicroclimatenetworkids0(_context):
    run_config = {}
    return run_config
