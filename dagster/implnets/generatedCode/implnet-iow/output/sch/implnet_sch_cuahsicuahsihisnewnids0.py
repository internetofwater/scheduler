from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisnewnids0 import implnet_job_cuahsicuahsihisnewnids0

@schedule(cron_schedule="0 22 4 * *", job=implnet_job_cuahsicuahsihisnewnids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisnewnids0(_context):
    run_config = {}
    return run_config
