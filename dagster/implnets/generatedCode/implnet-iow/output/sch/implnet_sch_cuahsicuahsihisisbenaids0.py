from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisisbenaids0 import implnet_job_cuahsicuahsihisisbenaids0

@schedule(cron_schedule="0 4 4 * *", job=implnet_job_cuahsicuahsihisisbenaids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisisbenaids0(_context):
    run_config = {}
    return run_config
