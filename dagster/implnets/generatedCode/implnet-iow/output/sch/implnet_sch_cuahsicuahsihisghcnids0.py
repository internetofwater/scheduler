from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisghcnids0 import implnet_job_cuahsicuahsihisghcnids0

@schedule(cron_schedule="0 8 6 * *", job=implnet_job_cuahsicuahsihisghcnids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisghcnids0(_context):
    run_config = {}
    return run_config
