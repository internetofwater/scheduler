from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisczomercedids0 import implnet_job_cuahsicuahsihisczomercedids0

@schedule(cron_schedule="0 0 1 * *", job=implnet_job_cuahsicuahsihisczomercedids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisczomercedids0(_context):
    run_config = {}
    return run_config
