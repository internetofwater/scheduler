from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihislittlebearriverids0 import implnet_job_cuahsicuahsihislittlebearriverids0

@schedule(cron_schedule="0 18 6 * *", job=implnet_job_cuahsicuahsihislittlebearriverids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihislittlebearriverids0(_context):
    run_config = {}
    return run_config
