from dagster import schedule, DefaultScheduleStatus

from jobs.implnet_jobs_cuahsicuahsihiscocorahsids0 import implnet_job_cuahsicuahsihiscocorahsids0

@schedule(cron_schedule="0 6 4 * *", job=implnet_job_cuahsicuahsihiscocorahsids0, default_status=DefaultScheduleStatus.RUNNING, execution_timezone="US/Central")
def implnet_sch_cuahsicuahsihiscocorahsids0(_context):
    run_config = {}
    return run_config
