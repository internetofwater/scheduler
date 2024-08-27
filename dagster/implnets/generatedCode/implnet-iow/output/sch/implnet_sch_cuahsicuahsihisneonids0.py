from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisneonids0 import implnet_job_cuahsicuahsihisneonids0

@schedule(cron_schedule="0 12 6 * *", job=implnet_job_cuahsicuahsihisneonids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisneonids0(_context):
    run_config = {}
    return run_config
