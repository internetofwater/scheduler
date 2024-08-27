from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisprovorivergamutids0 import implnet_job_cuahsihisprovorivergamutids0

@schedule(cron_schedule="0 8 3 * *", job=implnet_job_cuahsihisprovorivergamutids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisprovorivergamutids0(_context):
    run_config = {}
    return run_config
