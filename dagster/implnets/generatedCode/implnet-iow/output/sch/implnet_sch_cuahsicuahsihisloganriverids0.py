from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisloganriverids0 import implnet_job_cuahsicuahsihisloganriverids0

@schedule(cron_schedule="0 18 4 * *", job=implnet_job_cuahsicuahsihisloganriverids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisloganriverids0(_context):
    run_config = {}
    return run_config
