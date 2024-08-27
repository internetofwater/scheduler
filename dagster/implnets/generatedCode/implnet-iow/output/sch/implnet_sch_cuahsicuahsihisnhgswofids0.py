from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisnhgswofids0 import implnet_job_cuahsicuahsihisnhgswofids0

@schedule(cron_schedule="0 2 2 * *", job=implnet_job_cuahsicuahsihisnhgswofids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisnhgswofids0(_context):
    run_config = {}
    return run_config
