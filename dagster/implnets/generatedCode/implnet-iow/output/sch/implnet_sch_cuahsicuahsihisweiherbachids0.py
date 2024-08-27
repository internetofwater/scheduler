from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisweiherbachids0 import implnet_job_cuahsicuahsihisweiherbachids0

@schedule(cron_schedule="0 16 4 * *", job=implnet_job_cuahsicuahsihisweiherbachids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisweiherbachids0(_context):
    run_config = {}
    return run_config
