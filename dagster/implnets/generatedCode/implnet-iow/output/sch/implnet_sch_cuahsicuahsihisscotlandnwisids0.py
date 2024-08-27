from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisscotlandnwisids0 import implnet_job_cuahsicuahsihisscotlandnwisids0

@schedule(cron_schedule="0 12 4 * *", job=implnet_job_cuahsicuahsihisscotlandnwisids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisscotlandnwisids0(_context):
    run_config = {}
    return run_config
