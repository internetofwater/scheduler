from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisumbcwqids0 import implnet_job_cuahsicuahsihisumbcwqids0

@schedule(cron_schedule="0 22 5 * *", job=implnet_job_cuahsicuahsihisumbcwqids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisumbcwqids0(_context):
    run_config = {}
    return run_config
