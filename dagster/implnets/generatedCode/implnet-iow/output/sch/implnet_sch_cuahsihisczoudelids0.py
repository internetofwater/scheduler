from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsihisczoudelids0 import implnet_job_cuahsihisczoudelids0

@schedule(cron_schedule="0 6 1 * *", job=implnet_job_cuahsihisczoudelids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsihisczoudelids0(_context):
    run_config = {}
    return run_config
