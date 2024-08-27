from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisnewnids0 import implnet_job_cuahsihisnewnids0

@schedule(cron_schedule="0 22 4 * *", job=implnet_job_cuahsihisnewnids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisnewnids0(_context):
    run_config = {}
    return run_config
