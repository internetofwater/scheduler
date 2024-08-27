from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisloganriverids0 import implnet_job_cuahsihisloganriverids0

@schedule(cron_schedule="0 18 4 * *", job=implnet_job_cuahsihisloganriverids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisloganriverids0(_context):
    run_config = {}
    return run_config
