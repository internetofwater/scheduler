from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisnashrwaids0 import implnet_job_cuahsicuahsihisnashrwaids0

@schedule(cron_schedule="0 22 3 * *", job=implnet_job_cuahsicuahsihisnashrwaids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisnashrwaids0(_context):
    run_config = {}
    return run_config
