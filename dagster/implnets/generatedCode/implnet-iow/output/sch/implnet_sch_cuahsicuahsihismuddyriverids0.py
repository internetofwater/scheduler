from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihismuddyriverids0 import implnet_job_cuahsicuahsihismuddyriverids0

@schedule(cron_schedule="0 2 3 * *", job=implnet_job_cuahsicuahsihismuddyriverids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihismuddyriverids0(_context):
    run_config = {}
    return run_config
