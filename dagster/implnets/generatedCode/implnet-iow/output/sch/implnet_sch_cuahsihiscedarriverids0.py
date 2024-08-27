from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihiscedarriverids0 import implnet_job_cuahsihiscedarriverids0

@schedule(cron_schedule="0 18 3 * *", job=implnet_job_cuahsihiscedarriverids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihiscedarriverids0(_context):
    run_config = {}
    return run_config
