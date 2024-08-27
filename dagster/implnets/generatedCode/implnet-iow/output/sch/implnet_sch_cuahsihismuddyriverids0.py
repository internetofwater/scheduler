from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihismuddyriverids0 import implnet_job_cuahsihismuddyriverids0

@schedule(cron_schedule="0 2 3 * *", job=implnet_job_cuahsihismuddyriverids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihismuddyriverids0(_context):
    run_config = {}
    return run_config
