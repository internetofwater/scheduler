from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisnhgswofids0 import implnet_job_cuahsihisnhgswofids0

@schedule(cron_schedule="0 16 20 * *", job=implnet_job_cuahsihisnhgswofids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisnhgswofids0(_context):
    run_config = {}
    return run_config
