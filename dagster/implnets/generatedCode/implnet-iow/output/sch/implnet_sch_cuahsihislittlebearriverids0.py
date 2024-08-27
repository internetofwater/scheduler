from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihislittlebearriverids0 import implnet_job_cuahsihislittlebearriverids0

@schedule(cron_schedule="0 16 16 * *", job=implnet_job_cuahsihislittlebearriverids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihislittlebearriverids0(_context):
    run_config = {}
    return run_config
