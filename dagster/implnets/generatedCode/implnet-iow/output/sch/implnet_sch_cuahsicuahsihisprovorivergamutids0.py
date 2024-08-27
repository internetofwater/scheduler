from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisprovorivergamutids0 import implnet_job_cuahsicuahsihisprovorivergamutids0

@schedule(cron_schedule="0 8 3 * *", job=implnet_job_cuahsicuahsihisprovorivergamutids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisprovorivergamutids0(_context):
    run_config = {}
    return run_config
