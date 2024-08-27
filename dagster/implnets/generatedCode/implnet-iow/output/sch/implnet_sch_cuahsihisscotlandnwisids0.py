from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisscotlandnwisids0 import implnet_job_cuahsihisscotlandnwisids0

@schedule(cron_schedule="0 0 8 * *", job=implnet_job_cuahsihisscotlandnwisids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisscotlandnwisids0(_context):
    run_config = {}
    return run_config
