from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihiscedarriverids0 import implnet_job_cuahsicuahsihiscedarriverids0

@schedule(cron_schedule="0 18 3 * *", job=implnet_job_cuahsicuahsihiscedarriverids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihiscedarriverids0(_context):
    run_config = {}
    return run_config
