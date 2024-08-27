from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihisczoudelids0 import implnet_job_cuahsicuahsihisczoudelids0

@schedule(cron_schedule="0 6 1 * *", job=implnet_job_cuahsicuahsihisczoudelids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihisczoudelids0(_context):
    run_config = {}
    return run_config
