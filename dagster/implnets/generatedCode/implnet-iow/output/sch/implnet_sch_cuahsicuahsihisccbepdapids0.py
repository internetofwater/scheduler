from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisccbepdapids0 import implnet_job_cuahsicuahsihisccbepdapids0

@schedule(cron_schedule="0 22 1 * *", job=implnet_job_cuahsicuahsihisccbepdapids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisccbepdapids0(_context):
    run_config = {}
    return run_config
